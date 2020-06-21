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
package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RemoteLocationContext;

/**
 * A location in a remote namespace consisting of a nameservice ID and a HDFS
 * path (destination). It also contains the federated location (source).
 */
public class RemoteLocation extends RemoteLocationContext {

  /** Identifier of the remote namespace for this location. */
  private final String nameserviceId;
  /** Identifier of the namenode in the namespace for this location. */
  private final String namenodeId;
  /** Path in the remote location. */
  private final String dstPath;
  /** Original path in federation. */
  private final String srcPath;
  /** Priority for this location. */
  private final int priority;
  /** Is this location read only. */
  private final boolean readOnly;

  /**
   * Create a new remote location with dest path only.
   *
   * @param nsId  Destination namespace.
   * @param dPath Path in the destination namespace.
   */
  public RemoteLocation(String nsId, String dPath) {
    this(nsId, dPath, null);
  }

  /**
   * Create a new remote location.
   *
   * @param nsId  Destination namespace.
   * @param dPath Path in the destination namespace.
   * @param sPath Path in the federated level.
   */
  public RemoteLocation(String nsId, String dPath, String sPath) {
    this(nsId, null, dPath, sPath);
  }

  /**
   * Create a remote location point with priority and read-only attributes.
   *
   * @param nsId     Destination namespace.
   * @param dPath    Path in the destination namespace.
   * @param sPath    Path in the federated level
   * @param priority Priority for this location.
   * @param readOnly Is this location read only.
   */
  public RemoteLocation(String nsId, String dPath, String sPath, int priority,
                        boolean readOnly) {
    this(nsId, null, dPath, sPath, priority, readOnly);
  }

  /**
   * Create a new remote location pointing to a particular namenode in the
   * namespace.
   *
   * @param nsId Destination namespace.
   * @param nnId Destination namenode.
   * @param dPath Path in the destination namespace.
   * @param sPath Path in the federated level
   */
  public RemoteLocation(String nsId, String nnId, String dPath, String sPath) {
    String[] nsMsg = nsId.split(":", 3);
    this.nameserviceId = nsMsg[0];
    this.namenodeId = null;
    this.dstPath = dPath;
    this.srcPath = sPath;
    this.priority = nsMsg.length >= 2 ? Integer.parseInt(nsMsg[1]) : 0;
    this.readOnly = nsMsg.length >= 3 && Boolean.parseBoolean(nsMsg[2]);
  }

  /**
   * Create a remote location point with priority and read-only attributes.
   *
   * @param nsId     Destination namespace.
   * @param nnId     Destination namenode.
   * @param dPath    Path in the destination namespace.
   * @param sPath    Path in the federated level
   * @param priority Priority for this location.
   * @param readOnly Is this location read only.
   */
  public RemoteLocation(String nsId, String nnId, String dPath, String sPath,
                        int priority, boolean readOnly) {
    this.nameserviceId = nsId;
    this.namenodeId = nnId;
    this.dstPath = dPath;
    this.srcPath = sPath;
    this.priority = priority;
    this.readOnly = readOnly;
  }

  @Override
  public String getNameserviceId() {
    String ret = this.nameserviceId;
    if (this.namenodeId != null) {
      ret += "-" + this.namenodeId;
    }
    return ret;
  }

  @Override
  public String getDest() {
    return this.dstPath;
  }

  @Override
  public String getSrc() {
    return this.srcPath;
  }

  public int getPriority() {
    return this.priority;
  }

  public boolean isReadOnly() {
    return this.readOnly;
  }

  @Override
  public String toString() {
    return getNameserviceId() + "->" + this.dstPath + "[Priority:" +
        Integer.toString(this.priority) + "]" + "[ReadOnly:" +
        Boolean.toString(this.readOnly) + "]";
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(getNameserviceId())
        .append(getDest())
        .append(getPriority())
        .append(isReadOnly())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RemoteLocation) {
      RemoteLocation other = (RemoteLocation) obj;
      return this.getNameserviceId().equals(other.getNameserviceId()) &&
          this.getDest().equals(other.getDest()) &&
          this.getPriority() == other.getPriority() &&
          this.isReadOnly() == other.isReadOnly();
    }
    return false;
  }

  @Override
  public int compareTo(RemoteLocationContext info) {
    if (info instanceof RemoteLocation) {
      RemoteLocation other = (RemoteLocation) info;
      int ret = Integer.compare(this.getPriority(), other.getPriority());
      if (ret == 0) {
        return super.compareTo(info);
      } else {
        return ret;
      }
    } else {
      return super.compareTo(info);
    }
  }
}