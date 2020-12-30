/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;

import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * The AbfsInputStream for AbfsClient.
 */
public class AbfsInputStream extends FSInputStream implements CanUnbuffer,
        StreamCapabilities {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);

  private int readAheadBlockSize;
  private final AbfsClient client;
  private final Statistics statistics;
  private final String path;
  private final long contentLength;
  private final int bufferSize; // default buffer size
  private final int readAheadQueueDepth;         // initialized in constructor
  private final String eTag;                  // eTag of the path when InputStream are created
  private final boolean tolerateOobAppends; // whether tolerate Oob Appends
  private final boolean readAheadEnabled; // whether enable readAhead;
  private final boolean alwaysReadBufferSize;

  // SAS tokens can be re-used until they expire
  private CachedSASToken cachedSasToken;
  private byte[] buffer = null;            // will be initialized on first use

  private long fCursor = 0;  // cursor of buffer within file - offset of next byte to read from remote server
  private long fCursorAfterLastRead = -1;
  private int bCursor = 0;   // cursor of read within buffer - offset of next byte to be returned from buffer
  private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
  //                                                      of valid bytes in buffer)
  private boolean closed = false;

  /** Stream statistics. */
  private final AbfsInputStreamStatistics streamStatistics;
  private long bytesFromReadAhead; // bytes read from readAhead; for testing
  private long bytesFromRemoteRead; // bytes read remotely; for testing

  public AbfsInputStream(
          final AbfsClient client,
          final Statistics statistics,
          final String path,
          final long contentLength,
          final AbfsInputStreamContext abfsInputStreamContext,
          final String eTag) {
    this.client = client;
    this.statistics = statistics;
    this.path = path;
    this.contentLength = contentLength;
    this.bufferSize = abfsInputStreamContext.getReadBufferSize();
    this.readAheadQueueDepth = abfsInputStreamContext.getReadAheadQueueDepth();
    this.tolerateOobAppends = abfsInputStreamContext.isTolerateOobAppends();
    this.eTag = eTag;
    this.readAheadEnabled = true;
    this.alwaysReadBufferSize
        = abfsInputStreamContext.shouldReadBufferSizeAlways();
    this.cachedSasToken = new CachedSASToken(
        abfsInputStreamContext.getSasTokenRenewPeriodForStreamsInSeconds());
    this.streamStatistics = abfsInputStreamContext.getStreamStatistics();
    readAheadBlockSize = abfsInputStreamContext.getReadAheadBlockSize();

    // Propagate the config values to ReadBufferManager so that the first instance
    // to initialize can set the readAheadBlockSize
    ReadBufferManager.setReadBufferManagerConfigs(readAheadBlockSize);
  }

  public String getPath() {
    return path;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int numberOfBytesRead = read(b, 0, 1);
    if (numberOfBytesRead < 0) {
      return -1;
    } else {
      return (b[0] & 0xFF);
    }
  }

  @Override
  public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
    // check if buffer is null before logging the length
    if (b != null) {
      LOG.debug("read requested b.length = {} offset = {} len = {}", b.length,
          off, len);
    } else {
      LOG.debug("read requested b = null offset = {} len = {}", off, len);
    }

    int currentOff = off;
    int currentLen = len;
    int lastReadBytes;
    int totalReadBytes = 0;
    if (streamStatistics != null) {
      streamStatistics.readOperationStarted(off, len);
    }
    incrementReadOps();
    do {
      lastReadBytes = readOneBlock(b, currentOff, currentLen);
      if (lastReadBytes > 0) {
        currentOff += lastReadBytes;
        currentLen -= lastReadBytes;
        totalReadBytes += lastReadBytes;
      }
      if (currentLen <= 0 || currentLen > b.length - currentOff) {
        break;
      }
    } while (lastReadBytes > 0);
    return totalReadBytes > 0 ? totalReadBytes : lastReadBytes;
  }

  private int readOneBlock(final byte[] b, final int off, final int len) throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    Preconditions.checkNotNull(b);
    LOG.debug("read one block requested b.length = {} off {} len {}", b.length,
        off, len);

    if (len == 0) {
      return 0;
    }

    if (this.available() == 0) {
      return -1;
    }

    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }

    //If buffer is empty, then fill the buffer.
    if (bCursor == limit) {
      //If EOF, then return -1
      if (fCursor >= contentLength) {
        return -1;
      }

      long bytesRead = 0;
      //reset buffer to initial state - i.e., throw away existing data
      bCursor = 0;
      limit = 0;
      if (buffer == null) {
        LOG.debug("created new buffer size {}", bufferSize);
        buffer = new byte[bufferSize];
      }

      if (alwaysReadBufferSize) {
        bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
      } else {
        // Enable readAhead when reading sequentially
        if (-1 == fCursorAfterLastRead || fCursorAfterLastRead == fCursor || b.length >= bufferSize) {
          bytesRead = readInternal(fCursor, buffer, 0, bufferSize, false);
        } else {
          bytesRead = readInternal(fCursor, buffer, 0, b.length, true);
        }
      }

      if (bytesRead == -1) {
        return -1;
      }

      limit += bytesRead;
      fCursor += bytesRead;
      fCursorAfterLastRead = fCursor;
    }

    //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
    //(bytes returned may be less than requested)
    int bytesRemaining = limit - bCursor;
    int bytesToRead = Math.min(len, bytesRemaining);
    System.arraycopy(buffer, bCursor, b, off, bytesToRead);
    bCursor += bytesToRead;
    if (statistics != null) {
      statistics.incrementBytesRead(bytesToRead);
    }
    if (streamStatistics != null) {
      // Bytes read from the local buffer.
      streamStatistics.bytesReadFromBuffer(bytesToRead);
      streamStatistics.bytesRead(bytesToRead);
    }
    return bytesToRead;
  }


  private int readInternal(final long position, final byte[] b, final int offset, final int length,
                           final boolean bypassReadAhead) throws IOException {
    if (readAheadEnabled && !bypassReadAhead) {
      // try reading from read-ahead
      if (offset != 0) {
        throw new IllegalArgumentException("readahead buffers cannot have non-zero buffer offsets");
      }
      int receivedBytes;

      // queue read-aheads
      int numReadAheads = this.readAheadQueueDepth;
      long nextOffset = position;
      // First read to queue needs to be of readBufferSize and later
      // of readAhead Block size
      long nextSize = Math.min((long) bufferSize, contentLength - nextOffset);
      LOG.debug("read ahead enabled issuing readheads num = {}", numReadAheads);
      while (numReadAheads > 0 && nextOffset < contentLength) {
        LOG.debug("issuing read ahead requestedOffset = {} requested size {}",
            nextOffset, nextSize);
        ReadBufferManager.getBufferManager().queueReadAhead(this, nextOffset, (int) nextSize);
        nextOffset = nextOffset + nextSize;
        numReadAheads--;
        // From next round onwards should be of readahead block size.
        nextSize = Math.min((long) readAheadBlockSize, contentLength - nextOffset);
      }

      // try reading from buffers first
      receivedBytes = ReadBufferManager.getBufferManager().getBlock(this, position, length, b);
      bytesFromReadAhead += receivedBytes;
      if (receivedBytes > 0) {
        incrementReadOps();
        LOG.debug("Received data from read ahead, not doing remote read");
        if (streamStatistics != null) {
          streamStatistics.readAheadBytesRead(receivedBytes);
        }
        return receivedBytes;
      }

      // got nothing from read-ahead, do our own read now
      receivedBytes = readRemote(position, b, offset, length);
      return receivedBytes;
    } else {
      LOG.debug("read ahead disabled, reading remote");
      return readRemote(position, b, offset, length);
    }
  }

  int readRemote(long position, byte[] b, int offset, int length) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("attempting to read from negative offset");
    }
    if (position >= contentLength) {
      return -1;  // Hadoop prefers -1 to EOFException
    }
    if (b == null) {
      throw new IllegalArgumentException("null byte array passed in to read() method");
    }
    if (offset >= b.length) {
      throw new IllegalArgumentException("offset greater than length of array");
    }
    if (length < 0) {
      throw new IllegalArgumentException("requested read length is less than zero");
    }
    if (length > (b.length - offset)) {
      throw new IllegalArgumentException("requested read length is more than will fit after requested offset in buffer");
    }
    final AbfsRestOperation op;
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker, "readRemote", "read")) {
      LOG.trace("Trigger client.read for path={} position={} offset={} length={}", path, position, offset, length);
      op = client.read(path, position, b, offset, length, tolerateOobAppends ? "*" : eTag, cachedSasToken.get());
      cachedSasToken.update(op.getSasToken());
      if (streamStatistics != null) {
        streamStatistics.remoteReadOperation();
      }
      LOG.debug("issuing HTTP GET request params position = {} b.length = {} "
          + "offset = {} length = {}", position, b.length, offset, length);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
      incrementReadOps();
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException) {
        AbfsRestOperationException ere = (AbfsRestOperationException) ex;
        if (ere.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ere.getMessage());
        }
      }
      throw new IOException(ex);
    }
    long bytesRead = op.getResult().getBytesReceived();
    if (streamStatistics != null) {
      streamStatistics.remoteBytesRead(bytesRead);
    }
    if (bytesRead > Integer.MAX_VALUE) {
      throw new IOException("Unexpected Content-Length");
    }
    LOG.debug("HTTP request read bytes = {}", bytesRead);
    bytesFromRemoteRead += bytesRead;
    return (int) bytesRead;
  }

  /**
   * Increment Read Operations.
   */
  private void incrementReadOps() {
    if (statistics != null) {
      statistics.incrementReadOps(1);
    }
  }

  /**
   * Seek to given position in stream.
   * @param n position to seek to
   * @throws IOException if there is an error
   * @throws EOFException if attempting to seek past end of file
   */
  @Override
  public synchronized void seek(long n) throws IOException {
    LOG.debug("requested seek to position {}", n);
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    if (n < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (n > contentLength) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }

    if (streamStatistics != null) {
      streamStatistics.seek(n, fCursor);
    }

    if (n>=fCursor-limit && n<=fCursor) { // within buffer
      bCursor = (int) (n-(fCursor-limit));
      if (streamStatistics != null) {
        streamStatistics.seekInBuffer();
      }
      return;
    }

    // next read will read from here
    fCursor = n;
    LOG.debug("set fCursor to {}", fCursor);

    //invalidate buffer
    limit = 0;
    bCursor = 0;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    long currentPos = getPos();
    if (currentPos == contentLength) {
      if (n > 0) {
        throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
      }
    }
    long newPos = currentPos + n;
    if (newPos < 0) {
      newPos = 0;
      n = newPos - currentPos;
    }
    if (newPos > contentLength) {
      newPos = contentLength;
      n = newPos - currentPos;
    }
    seek(newPos);
    return n;
  }

  /**
   * Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   *
   * This is to match the behavior of DFSInputStream.available(),
   * which some clients may rely on (HBase write-ahead log reading in
   * particular).
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException(
          FSExceptionMessages.STREAM_IS_CLOSED);
    }
    final long remaining = this.contentLength - this.getPos();
    return remaining <= Integer.MAX_VALUE
        ? (int) remaining : Integer.MAX_VALUE;
  }

  /**
   * Returns the length of the file that this stream refers to. Note that the length returned is the length
   * as of the time the Stream was opened. Specifically, if there have been subsequent appends to the file,
   * they wont be reflected in the returned length.
   *
   * @return length of the file.
   * @throws IOException if the stream is closed
   */
  public long length() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    return contentLength;
  }

  /**
   * Return the current offset from the start of the file
   * @throws IOException throws {@link IOException} if there is an error
   */
  @Override
  public synchronized long getPos() throws IOException {
    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
    return fCursor - limit + bCursor;
  }

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   * @throws IOException throws {@link IOException} if there is an error
   */
  @Override
  public boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    buffer = null; // de-reference the buffer so it can be GC'ed sooner
    LOG.debug("Closing {}", this);
  }

  /**
   * Not supported by this stream. Throws {@link UnsupportedOperationException}
   * @param readlimit ignored
   */
  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
  }

  /**
   * Not supported by this stream. Throws {@link UnsupportedOperationException}
   */
  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
  }

  /**
   * gets whether mark and reset are supported by {@code ADLFileInputStream}. Always returns false.
   *
   * @return always {@code false}
   */
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    buffer = null;
    // Preserve the original position returned by getPos()
    fCursor = fCursor - limit + bCursor;
    fCursorAfterLastRead = -1;
    bCursor = 0;
    limit = 0;
  }

  @Override
  public boolean hasCapability(String capability) {
    return StreamCapabilities.UNBUFFER.equals(toLowerCase(capability));
  }

  byte[] getBuffer() {
    return buffer;
  }

  @VisibleForTesting
  protected void setCachedSasToken(final CachedSASToken cachedSasToken) {
    this.cachedSasToken = cachedSasToken;
  }

  /**
   * Getter for AbfsInputStreamStatistics.
   *
   * @return an instance of AbfsInputStreamStatistics.
   */
  @VisibleForTesting
  public AbfsInputStreamStatistics getStreamStatistics() {
    return streamStatistics;
  }

  /**
   * Getter for bytes read from readAhead buffer that fills asynchronously.
   *
   * @return value of the counter in long.
   */
  @VisibleForTesting
  public long getBytesFromReadAhead() {
    return bytesFromReadAhead;
  }

  /**
   * Getter for bytes read remotely from the data store.
   *
   * @return value of the counter in long.
   */
  @VisibleForTesting
  public long getBytesFromRemoteRead() {
    return bytesFromRemoteRead;
  }

  @VisibleForTesting
  public int getBufferSize() {
    return bufferSize;
  }

  @VisibleForTesting
  public int getReadAheadQueueDepth() {
    return readAheadQueueDepth;
  }

  @VisibleForTesting
  public boolean shouldAlwaysReadBufferSize() {
    return alwaysReadBufferSize;
  }

  /**
   * Get the statistics of the stream.
   * @return a string value.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    if (streamStatistics != null) {
      sb.append("AbfsInputStream@(").append(this.hashCode()).append("){");
      sb.append(streamStatistics.toString());
      sb.append("}");
    }
    return sb.toString();
  }
}
