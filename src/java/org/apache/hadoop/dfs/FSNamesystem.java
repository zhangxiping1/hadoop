/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/***************************************************
 * The FSNamesystem tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
public class FSNamesystem implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.fs.FSNamesystem");

   

    //
    // Stores the correct file name hierarchy
    //
    FSDirectory dir;

    //
    // Stores the block-->datanode(s) map.  Updated only in response
    // to client-sent information.
    //
    TreeMap blocksMap = new TreeMap(); // 对应的目录树中存在文件，dir中activeBlocks集合才会存在block  与blockMaps区别在于，blockMaps包括正在写的文件已完成的block

    //
    // Stores the datanode-->block map.  Done by storing a 
    // set of datanode info objects, sorted by name.  Updated only in
    // response to client-sent information.
    //
    TreeMap datanodeMap = new TreeMap();

    //
    // Keeps a Vector for every named machine.  The Vector contains
    // blocks that have recently been invalidated and are thought to live
    // on the machine in question.
    //` invalidate `列表用来通知datanode该数据块应该被删除。在向namenode发出指令后，元素会从无效列表中删除。
    // 添加有两种情况：1.超过最大副本数，2.删除文件时，会将文件的所有块放入invalidate列表中
    // 什么时候删除呢？当datanode向namenode发送心跳getBlockwork时
    TreeMap recentInvalidateSets = new TreeMap();

    //
    // Keeps a TreeSet for every named node.  Each treeset contains
    // a list of the blocks that are "extra" at that location.  We'll
    // eventually remove these extras.
    // 冗余副本列表，用来存储超过最大副本数的数据块
    TreeMap excessReplicateMap = new TreeMap();

    //
    // Keeps track of files that are being created, plus the
    // blocks that make them up.
    //
    TreeMap pendingCreates = new TreeMap();

    //
    // Keeps track of the blocks that are part of those pending creates
    //
    // pendingCreate文件所包含的块，可能这个文件10个块，但是文件还没有创建完成，只有5个块已经创建完成，那么这个文件就会有5个块在这个列表中
    TreeSet pendingCreateBlocks = new TreeSet();

    //
    // Stats on overall usage
    //
    long totalCapacity = 0, totalRemaining = 0;

    //
    Random r = new Random();

    //
    // Stores a set of datanode info objects, sorted by heartbeat
    //
    TreeSet heartbeats = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            DatanodeInfo d1 = (DatanodeInfo) o1;
            DatanodeInfo d2 = (DatanodeInfo) o2;            
            long lu1 = d1.lastUpdate();
            long lu2 = d2.lastUpdate();
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return d1.getName().compareTo(d2.getName());
            }
        }
    });

    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    //
    private TreeSet neededReplications = new TreeSet();
    private TreeSet pendingReplications = new TreeSet();

    //
    // Used for handling lock-leases
    //
    private TreeMap leases = new TreeMap();
    private TreeSet sortedLeases = new TreeSet();

    //
    // Threaded object that checks to see if we have been
    // getting heartbeats from all clients. 
    //
    HeartbeatMonitor hbmon = null;
    LeaseMonitor lmon = null;
    Daemon hbthread = null, lmthread = null;
    boolean fsRunning = true;
    long systemStart = 0;
    private Configuration conf;

    //  DESIRED_REPLICATION is how many copies we try to have at all times
    private int desiredReplication;
    //  The maximum number of replicates we should allow for a single block
    private int maxReplication;
    //  How many outgoing replication streams a given node should have at one time
    private int maxReplicationStreams;
    // MIN_REPLICATION is how many copies we need in place or else we disallow the write
    private int minReplication;
    // HEARTBEAT_RECHECK is how often a datanode sends its hearbeat
    private int heartBeatRecheck;
   //  Whether we should use disk-availability info when determining target
    private boolean useAvailability;

    private boolean allowSameHostTargets;
    
    /**
     * dir is where the filesystem directory state 
     * is stored
     */
    public FSNamesystem(File dir, Configuration conf) throws IOException {
        this.dir = new FSDirectory(dir);
        this.hbthread = new Daemon(new HeartbeatMonitor());
        this.lmthread = new Daemon(new LeaseMonitor());
        hbthread.start();
        lmthread.start();
        this.systemStart = System.currentTimeMillis();
        this.conf = conf;
        
        this.desiredReplication = conf.getInt("dfs.replication", 3);
        this.maxReplication = desiredReplication;
        this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
        this.minReplication = 1;
        this.heartBeatRecheck= 1000;
        this.useAvailability = conf.getBoolean("dfs.availability.allocation", false);
        this.allowSameHostTargets =
           conf.getBoolean("test.dfs.same.host.targets.allowed", false);
    }

    /** Close down this filesystem manager.
     * Causes heartbeat and lease daemons to stop; waits briefly for
     * them to finish, but a short timeout returns control back to caller.
     */
    public void close() {
      synchronized (this) {
        fsRunning = false;
      }
        try {
            hbthread.join(3000);
        } catch (InterruptedException ie) {
        } finally {
          // using finally to ensure we also wait for lease daemon
          try {
            lmthread.join(3000);
          } catch (InterruptedException ie) {
          }
        }
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by HadoopFS clients
    //
    /////////////////////////////////////////////////////////
    /**
     * The client wants to open the given filename.  Return a
     * list of (block,machineArray) pairs.  The sequence of unique blocks
     * in the list indicates all the blocks that make up the filename.
     *
     * The client should choose one of the machines from the machineArray
     * at random.
     */
    public Object[] open(UTF8 src) {
        Object results[] = null;
        Block blocks[] = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeInfo machineSets[][] = new DatanodeInfo[blocks.length][];

            for (int i = 0; i < blocks.length; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];
                    int j = 0;
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = (DatanodeInfo) it.next();
                    }
                }
            }

            results[0] = blocks;
            results[1] = machineSets;
        }
        return results;
    }

    /**
     * The client would like to create a new block for the indicated
     * filename.  Return an array that consists of the block, plus a set 
     * of machines.  The first on this list should be where the client 
     * writes data.  Subsequent items in the list must be provided in
     * the connection to the first datanode.
     * @return Return an array that consists of the block, plus a set
     * of machines, or null if src is invalid for creation (based on
     * {@link FSDirectory#isValidToCreate(UTF8)}.
     */
    public synchronized Object[] startFile(UTF8 src, UTF8 holder, boolean overwrite) {
        Object results[] = null;
        if (pendingCreates.get(src) == null) {
            boolean fileValid = dir.isValidToCreate(src);
            if (overwrite && ! fileValid) {
                delete(src);
                fileValid = true;
            }

            if (fileValid) {
                results = new Object[2];

                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null);
                LOG.info("******** creating file " + src +",(1).chooseTargets 选取DN节点："+ Arrays.stream(targets).toArray().toString());
                if (targets.length < this.minReplication) {
                    LOG.warning("Target-length is " + targets.length +
                        ", below MIN_REPLICATION (" + this.minReplication+ ")");
                    return null;
                }

                // Reserve space for this pending file
                LOG.info("******** creating file "+ src +",(2).添加到 pendingCreates{src, new Vector(blocks)}");
                pendingCreates.put(src, new Vector());
                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease == null) {
                        lease = new Lease(holder);
                        LOG.info("******** creating file "+ src +",(3)client没有租约，申请新租约lease:"+ lease+",放入到 leases,sortedLeases");
                        leases.put(holder, lease);
                        sortedLeases.add(lease);
                    } else {
                        LOG.info("******** creating file "+ src +",(3)client有租约，更新租约lease:"+ lease);
                        sortedLeases.remove(lease);
                        lease.renew();
                        sortedLeases.add(lease);
                    }
                    LOG.info("******** creating file "+ src +",(4)lease:"+ lease +"添加正在被创建的文件,维持正在create的多个文件");
                    lease.startedCreate(src);
                }

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            } else { // ! fileValid
              LOG.warning("Cannot start file because it is invalid. src=" + src);
            }
        } else {
            LOG.warning("Cannot start file because pendingCreates is non-null. src=" + src);
        }
        return results;
    }

    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     *
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src) {
        Object results[] = null;
        LOG.info("******** creating file "+ src +",(7).继续添加block，检查目录树dir不应存在改文件（已完成的文件才会放入dir）,pendingCreates正在被创建文件列表需要包含 : "+ src);
        if (dir.getFile(src) == null && pendingCreates.get(src) != null) {
            results = new Object[2];

            //
            // If we fail this, bad things happen!

            //
            if (checkFileProgress(src)) {
                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null);
                if (targets.length < this.minReplication) {
                    return null;
                }

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            }
        }
        return results;
    }

    /**
     * The client would like to let go of the given block
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        //
        // Remove the block from the pending creates list
        //
        Vector pendingVector = (Vector) pendingCreates.get(src);
        if (pendingVector != null) {
            for (Iterator it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
                if (cur.compareTo(b) == 0) {
                    pendingCreateBlocks.remove(cur);
                    it.remove();
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Abandon the entire file in progress
     */
    public synchronized void abandonFileInProgress(UTF8 src) throws IOException {
        internalReleaseCreate(src);
    }

    /**
     * Finalize the created file and make it world-accessible.  The
     * FSNamesystem will already know the blocks that make up the file.
     * Before we return, we make sure that all the file's blocks have 
     * been reported by datanodes and are replicated correctly.
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
	    LOG.info("Failed to complete " + src + "  because dir.getFile()==" + dir.getFile(src) + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (! checkFileProgress(src)) {
            return STILL_WAITING;
        } else {
            Vector pendingVector = (Vector) pendingCreates.get(src);
            Block pendingBlocks[] = (Block[]) pendingVector.toArray(new Block[pendingVector.size()]);

            //
            // We have the pending blocks, but they won't have
            // length info in them (as they were allocated before
            // data-write took place).  So we need to add the correct
            // length info to each
            //
            // REMIND - mjc - this is very inefficient!  We should
            // improve this!
            // blocksMap


            LOG.info("******** completeFile src:"+src+" 操作：pendingCreates维持了{src -> blocks},completeFile 这里会查询blocks(从 blocksMap) 里面的块的实际大小，然后修改块的大小，将它放入目录树中");
            LOG.info("******** completeFile,blocksMap 这个结构是(client 写完块汇报/DN增量汇报)时修改，每次都遍历这个大集合耗性能: "+ src);
            for (int i = 0; i < pendingBlocks.length; i++) {
                Block b = pendingBlocks[i];
                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                DatanodeInfo node = (DatanodeInfo) containingNodes.first();
                for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
                    Block cur = (Block) it.next();
                    if (b.getBlockId() == cur.getBlockId()) {
                        b.setNumBytes(cur.getNumBytes());
                        break;
                    }
                }
            }
            LOG.info("******** 首先blocksMap 这个结构是DN快汇报时修改，这个文件pendingBlocks为什么需要设置块的大小？原来pendingCreates有{src -> blocks}这个映射关系，添加到目录树需要这个结构数据");
            
            //
            // Now we can add the (name,blocks) tuple to the filesystem
            //
            LOG.info("******** creating file "+ src +",(11).completeFile,pendingCreates维持了{src -> blocks} 信息 写入到 目录数dir");
            if (dir.addFile(src, pendingBlocks)) {
                // The file is no longer pending
                pendingCreates.remove(src);
                LOG.info("******** creating file "+ src +",(12).文件已写入目录树，移除pendingCreates{src -> blocks} src信息 ");
                for (int i = 0; i < pendingBlocks.length; i++) {
                    LOG.info("******** creating file "+ src +",(13).文件已写入目录树，移除pendingCreateBlocks{blocks} block信息："+pendingBlocks[i]);
                    pendingCreateBlocks.remove(pendingBlocks[i]);
                }

                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease != null) {
                        lease.completedCreate(src);
                        LOG.info("******** creating file "+ src +",(13).租约Create文件列表移除该文件信息");
                        if (! lease.hasLocks()) {
                            LOG.info("******** creating file "+ src +",(14).租约Create文件列表空，移除该租约信息");
                            leases.remove(holder);
                            sortedLeases.remove(lease);
                        }
                    }
                }

                //
                // REMIND - mjc - this should be done only after we wait a few secs.
                // The namenode isn't giving datanodes enough time to report the
                // replicated blocks that are automatically done as part of a client
                // write.
                //

                // Now that the file is real, we need to be sure to replicate
                // the blocks.
                for (int i = 0; i < pendingBlocks.length; i++) {
                    TreeSet containingNodes = (TreeSet) blocksMap.get(pendingBlocks[i]);
                    if (containingNodes.size() < this.desiredReplication) {
                        synchronized (neededReplications) {
                            LOG.info("Completed file " + src + ", at holder " + holder + ".  There is/are only " + containingNodes.size() + " copies of block " + pendingBlocks[i] + ", so replicating up to " + this.desiredReplication);
                            neededReplications.add(pendingBlocks[i]);
                        }
                    }
                }
                return COMPLETE_SUCCESS;
            } else {
                System.out.println("AddFile() for " + src + " failed");
            }
	    LOG.info("Dropped through on file add....");
        }

        return OPERATION_FAILED;
    }

    /**
     * Allocate a block at the given pending filename
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = new Block();
        Vector v = (Vector) pendingCreates.get(src);
        LOG.info("******** creating file "+ src +",(5)申请了一个新块返回给client,block:"+b+"，放入到pendingCreates (src -> V集合(正在等待创建的块))和pendingCreateBlocks(正在等待创建的块)");
        v.add(b);
        pendingCreateBlocks.add(b);
        return b;
    }

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false.
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        Vector v = (Vector) pendingCreates.get(src);
        LOG.info("******** creating file "+ src +",(8)检查文件前面写的所有块副本是否已达标(containingNodes.size>minReplication)，正常块完成后是会放入到blocksMap集合{block -> datanode(s)}，副本可能没有达标");
        for (Iterator it = v.iterator(); it.hasNext(); ) {
            Block b = (Block) it.next();
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }
        return true;
    }

    ////////////////////////////////////////////////////////////////
    // Here's how to handle block-copy failure during client write:
    // -- As usual, the client's write should result in a streaming
    // backup write to a k-machine sequence.
    // -- If one of the backup machines fails, no worries.  Fail silently.
    // -- Before client is allowed to close and finalize file, make sure
    // that the blocks are backed up.  Namenode may have to issue specific backup
    // commands to make up for earlier datanode failures.  Once all copies
    // are made, edit namespace and return to client.
    ////////////////////////////////////////////////////////////////

    /**
     * Change the indicated filename.
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        return dir.renameTo(src, dst);
    }

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) {
        Block deletedBlocks[] = (Block[]) dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];

                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeInfo node = (DatanodeInfo) it.next();
                        Vector invalidateSet = (Vector) recentInvalidateSets.get(node.getName());
                        if (invalidateSet == null) {
                            invalidateSet = new Vector();
                            recentInvalidateSets.put(node.getName(), invalidateSet);
                        }
                        invalidateSet.add(b);
                    }
                }
            }
        }

        return (deletedBlocks != null);
    }

    /**
     * Return whether the given filename exists
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Whether the given name is a directory
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * Create all the necessary directories
     */
    public boolean mkdirs(UTF8 src) {
        return dir.mkdirs(src);
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block(s) referred to by the given (filename, start, len) tuple.
     */
    public UTF8[][] getDatanodeHints(UTF8 src, long start, long len) {
        if (start < 0 || len < 0) {
            return new UTF8[0][];
        }

        int startBlock = -1;
        int endBlock = -1;
        Block blocks[] = dir.getFile(src);

        //
        // First, figure out where the range falls in
        // the blocklist.
        //
        long startpos = start;
        long endpos = start + len;
        for (int i = 0; i < blocks.length; i++) {
            if (startpos >= 0) {
                startpos -= blocks[i].getNumBytes();
                if (startpos <= 0) {
                    startBlock = i;
                }
            }
            if (endpos >= 0) {
                endpos -= blocks[i].getNumBytes();
                if (endpos <= 0) {
                    endBlock = i;
                    break;
                }
            }
        }

        //
        // Next, create an array of hosts where each block can
        // be found
        //
        if (startBlock < 0 || endBlock < 0) {
            return new UTF8[0][];
        } else {
            UTF8 hosts[][] = new UTF8[endBlock - startBlock + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                Vector v = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    v.add(cur.getHost());
                }
                hosts[i] = (UTF8[]) v.toArray(new UTF8[v.size()]);
            }
            return hosts;
        }
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        TreeSet locks = new TreeSet();
        TreeSet creates = new TreeSet();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }
        public void renew() {
            this.lastUpdate = System.currentTimeMillis();
        }
        public boolean expired() {
            if (System.currentTimeMillis() - lastUpdate > LEASE_PERIOD) {
                return true;
            } else {
                return false;
            }
        }
        public void obtained(UTF8 src) {
            locks.add(src);
        }
        public void released(UTF8 src) {
            locks.remove(src);
        }
        public void startedCreate(UTF8 src) {
            creates.add(src);
        }
        public void completedCreate(UTF8 src) {
            creates.remove(src);
        }
        public boolean hasLocks() {
            return (locks.size() + creates.size()) > 0;
        }
        public void releaseLocks() {
            for (Iterator it = locks.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseLock(src, holder);
            }
            locks.clear();
            for (Iterator it = creates.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseCreate(src);
            }
            creates.clear();
        }

        /**
         */
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " + locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        /**
         */
        public int compareTo(Object o) {
            Lease l1 = (Lease) this;
            Lease l2 = (Lease) o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }
    }
    /******************************************************
     * LeaseMonitor checks for leases that have expired,
     * and disposes of them.
     ******************************************************/
    class LeaseMonitor implements Runnable {
        public void run() {
            while (fsRunning) {
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                               ((top = (Lease) sortedLeases.first()) != null)) {
                            if (top.expired()) {
                                top.releaseLocks();
                                leases.remove(top.holder);
                                LOG.info("Removing lease " + top + ", leases remaining: " + sortedLeases.size());
                                if (!sortedLeases.remove(top)) {
                                    LOG.info("Unknown failure trying to remove " + top + " from lease set.");
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Get a lock (perhaps exclusive) on the given file
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease == null) {
                    lease = new Lease(holder);
                    leases.put(holder, lease);
                    sortedLeases.add(lease);
                } else {
                    sortedLeases.remove(lease);
                    lease.renew();
                    sortedLeases.add(lease);
                }
                lease.obtained(src);
            }
        }
        return result;
    }

    /**
     * Release the lock on the given file
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease != null) {
                    lease.released(src);
                    if (! lease.hasLocks()) {
                        leases.remove(holder);
                        sortedLeases.remove(lease);
                    }
                }
            }
        }
        return result;
    }
    private int internalReleaseLock(UTF8 src, UTF8 holder) {
        return dir.releaseLock(src, holder);
    }
    private void internalReleaseCreate(UTF8 src) {
        Vector v = (Vector) pendingCreates.remove(src);
        for (Iterator it2 = v.iterator(); it2.hasNext(); ) {
            Block b = (Block) it2.next();
            //这里为什么没有直接移除 blockMaps里面的 ，还有DNinfo 里面的 （下面的DN块汇报（有这些无效的块）会添加回来 ，（冲突了！））
            // The new report has a block the old one does not
            //  addStoredBlock(newReport[newPos], node);

            pendingCreateBlocks.remove(b); }
    }

    /**
     * Renew the lease(s) held by the given client
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    /**
     * Get a listing of all files at 'src'.  The Object[] array
     * exists so we can return file attributes (soon to be implemented)
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by datanodes
    //
    /////////////////////////////////////////////////////////
    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     */
    public void gotHeartbeat(UTF8 name, long capacity, long remaining) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                long capacityDiff = 0;
                long remainingDiff = 0;
                DatanodeInfo nodeinfo = (DatanodeInfo) datanodeMap.get(name);

                if (nodeinfo == null) {
                    LOG.info("Got brand-new heartbeat from " + name);
                    nodeinfo = new DatanodeInfo(name, capacity, remaining);
                    datanodeMap.put(name, nodeinfo);
                    capacityDiff = capacity;
                    remainingDiff = remaining;
                } else {
                    capacityDiff = capacity - nodeinfo.getCapacity();
                    remainingDiff = remaining - nodeinfo.getRemaining();
                    heartbeats.remove(nodeinfo);
                    nodeinfo.updateHeartbeat(capacity, remaining);
                }
                heartbeats.add(nodeinfo);
                totalCapacity += capacityDiff;
                totalRemaining += remainingDiff;
            }
        }
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {
        /**
         */
        public void run() {
            while (fsRunning) {
                heartbeatCheck();
                try {
                    Thread.sleep(heartBeatRecheck);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     */
    synchronized void heartbeatCheck() {
        synchronized (heartbeats) {
            DatanodeInfo nodeInfo = null;

            while ((heartbeats.size() > 0) &&
                   ((nodeInfo = (DatanodeInfo) heartbeats.first()) != null) &&
                   (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)) {
                LOG.info("Lost heartbeat for " + nodeInfo.getName());

                heartbeats.remove(nodeInfo);
                synchronized (datanodeMap) {
                    datanodeMap.remove(nodeInfo.getName());
                }
                totalCapacity -= nodeInfo.getCapacity();
                totalRemaining -= nodeInfo.getRemaining();

                Block deadblocks[] = nodeInfo.getBlocks();
                if (deadblocks != null) {
                    for (int i = 0; i < deadblocks.length; i++) {
                        removeStoredBlock(deadblocks[i], nodeInfo);
                    }
                }

                if (heartbeats.size() > 0) {
                    nodeInfo = (DatanodeInfo) heartbeats.first();
                }
            }
        }
    }
    
    /**
     * The given node is reporting all its blocks.  Use this info to 
     * update the (machine-->blocklist) and (block-->machinelist) tables.
     */
    public synchronized Block[] processReport(Block newReport[], UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Received block report from node " + name + ", but there is no info for " + name);
        }

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int oldPos = 0, newPos = 0;
        Block oldReport[] = node.getBlocks();
        LOG.info("********* processReport 处理DN块汇报信息："+name.toString());
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);
            // 这里可以判断到TreeSet oldReport 与 newReport的blockid是按照升序排列的
            // 这里在处理块汇报，拿DN内存里面 blocks集合（一方面块汇报形成，一方面是写入过程添加的）与新汇报的blocks集合进行比较，如果相同脚标的block相同就不做处理，如果不同就进行处理
            if (cmp == 0) {                               //old DN{b1,b2,b3,b5}  new DN{b1,b2,b4,b6}
                // Do nothing, blocks are the same
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // The old report has a block the new one does not
                //// 这里在处理块汇报，拿DN内存里面 blocks集合与新汇报的blocks集合进行比较，如果相同脚标的old blockid小于new blockid，就删除old blockid
                removeStoredBlock(oldReport[oldPos], node);  //old DN{b1,b2,b3,b5}  new DN{b1,b2,b4,b6}  blocksMap 删除 {b3 dn}
                                                                            // ↑                  ↑
                oldPos++;
            } else {                                         //上面会走到这里 就会添加 {b4 dn}   //说道理就是以新的为准呗 ，新汇报的块有，blocksMap就添加，没有，blocksMap就删除
                // The new report has a block the old one does not
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        while (oldReport != null && oldPos < oldReport.length) {   //old DN{b1,b2,b3,b5}  new DN{b1,b2}  //老的没遍历完，就把NN内存里老的后面删除（这个不太合理，如果DN文件丢失了，重启后就有问题）
            // The old report has a block the new one does not
            removeStoredBlock(oldReport[oldPos], node);
            LOG.info("********* 不合理的删除，processReport 删除DN块："+oldReport[oldPos].toString());
            oldPos++;
        }
        while (newReport != null && newPos < newReport.length) {  //old DN{b1,b2}  new DN{b1,b2,b3,b5}  //新的没遍历完，就把新的后面都加进来
            // The new report has a block the old one does not
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }

        //
        // Modify node so it has the new blockreport
        //
        node.updateBlocks(newReport);

        //
        // We've now completely updated the node's block report profile.
        // We now go through all its blocks and find which ones are invalid,
        // no longer pending, or over-replicated.
        //
        // (Note it's not enough to just invalidate blocks at lease expiry 
        // time; datanodes can go down before the client's lease on 
        // the failed file expires and miss the "expire" event.)
        //
        // This function considers every block on a datanode, and thus
        // should only be invoked infrequently.
        //我们现在已经完全更新了节点的块报告。我们现在遍历它的所有块，找出哪些是无效的、不再挂起的或过度复制的。
        //(注意，仅仅在过期时间内使块失效是不够的;datanode可能会在失败文件的租约到期之前宕机，从而错过“expire”事件。) （全量汇报意义：1.dn数据丢失）
        // 他这个意思是（失效的块产生：1.主动删除产生的 ，2.DN宕机nn自动恢复副本，dn重启产生的 ，这些都会被getBlockwork调用删除）这个操作不足以清除无效的块
        // 有一种场景：客户端写文件，写了一半，客户端宕机，文件写失败，这时候这些已经写完的块就是过时的，（目前无法通知DN清理）DN上存在这些过时的块，需要在下面的代码清理


        //为什么判断pendingCreateBlocks.contains?： pendingCreate文件所包含的块，可能这个文件10个块，但是文件还没有创建完成，只有5个块已经创建完成（保存到blockMap），那么这个文件就会有5个块在这个列表中 ，这时DN上有五个块，
        //当客户端正在写这个文件时，刚好这个DN(已经有完成的块)宕机了，租约到期会清理这些pendingCreateBlocks ，如果这个DN在过期前恢复了，并且DN上面有正在写的块，
        // 如果不判断这个块是否在pendingCreateBlocks中，就会把这个块误删除了 ，如果客户端发生错误，这个DN在租约过期后恢复，就会正常删除

        //该函数考虑datanode上的每个数据块，因此
        //应该只在不频繁的情况下调用。
        Vector obsolete = new Vector();
        //就这个循环要把集群搞死 ，遍历这个DN所有块，然后查询集群所有activeBlocks
        for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = (Block) it.next();

            if (! dir.isValidBlock(b) && ! pendingCreateBlocks.contains(b)) {
                LOG.info("Obsoleting block " + b);
                obsolete.add(b);
            }
        }
        return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
    }

    /**
     * Modify (block-->datanode) map.  Remove block from set of 
     * needed replications if this takes care of the problem.
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet();
//            new Exception("************* blocksMap 添加块信息：block:"+ block + ",node:" +node).printStackTrace();
            blocksMap.put(block, containingNodes);
        }
        if (! containingNodes.contains(node)) {
            containingNodes.add(node);
        } else {
            LOG.info("Redundant addStoredBlock request received for block " + block + " on node " + node);
        }
        LOG.info("******** blocksMap保存块和节点的映射关系，block："+ block + ",node:" +node);
          // 果然是增量汇报 ，移除neededReplications 集合数据。
        synchronized (neededReplications) {
            if (dir.isValidBlock(block)) {
                if (containingNodes.size() >= this.desiredReplication) {
                    LOG.info("******** 满足指定的副本数block:"+block+",从待复制集合移除neededReplications,pendingReplications");
                    neededReplications.remove(block);
                    pendingReplications.remove(block);
                } else if (containingNodes.size() < this.desiredReplication) {
                    if (! neededReplications.contains(block)) {
                        LOG.info("******** 不满足指定的副本数block:"+block+",添加待复制集合neededReplications");
                        neededReplications.add(block);
                    }
                }

                //
                // Find how many of the containing nodes are "extra", if any.
                // If there are any extras, call chooseExcessReplicates() to
                // mark them in the excessReplicateMap.
                //下面这个逻辑是，每次添加blocksMap {block DNs}时，判断是否超过最大副本数，如果超过就调用chooseExcessReplicates()方法，将超过的副本数放入excessReplicateMap
                //正常情况下，只要比较containingNodes.size() >= this.desiredReplication就可以了，但是这里多了一个excessReplicateMap ，试想一下这个场景，集群有五个DN，
                // 一个块有三个副本，当正好两个块上的DN停止，然后就会恢复到其他两个节点，这样就会出现一个块有五个副本，然后两台启动起来汇报，第一台汇报处理时这里就会计数，有四个 4-0 ，并且会将这个块随机选个DN放入excessReplicateMap
                // 第二个启动时汇报，也会走到这里，这里也会遍历五个节点，然后发现这个块有个随机选的DN已经在excessReplicateMap里面了，并且包含这个块，这里还是会计数nonExcess.size=4，5-1
                // 这个集合excessReplicateMap的目的就是排除这个块可能前面已经有冗余的DN
                Vector nonExcess = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
                    if (excessBlocks == null || ! excessBlocks.contains(block)) { //正常第一次改DN excessBlocks == null 成立，
                        nonExcess.add(cur);
                    }
                }
                if (nonExcess.size() > this.maxReplication) {
                    chooseExcessReplicates(nonExcess, block, this.maxReplication);    
                }
            }
        }
    }

    /**
     * We want a max of "maxReps" replicates for any block, but we now have too many.  
     * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
     *
     * srcNodes.size() - dstNodes.size() == maxReps
     *
     * For now, we choose nodes randomly.  In the future, we might enforce some
     * kind of policy (like making sure replicates are spread across racks).
     */
    void chooseExcessReplicates(Vector nonExcess, Block b, int maxReps) {
        while (nonExcess.size() - maxReps > 0) {
            int chosenNode = r.nextInt(nonExcess.size()); //随机选择一个DN
            DatanodeInfo cur = (DatanodeInfo) nonExcess.elementAt(chosenNode);
            nonExcess.removeElementAt(chosenNode);

            TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
            if (excessBlocks == null) { // 直接放进去 excessReplicateMap
                excessBlocks = new TreeSet();
                excessReplicateMap.put(cur.getName(), excessBlocks);
            }
            excessBlocks.add(b); //放入excessReplicateMap {DN -> TreeSet{block1,block2,block3...}}

            //
            // The 'excessblocks' tracks blocks until we get confirmation
            // that the datanode has deleted them; the only way we remove them
            // is when we get a "removeBlock" message.  
            //` excessblocks `会跟踪数据块，直到我们确认datanode已经删除了它们;我们删除它们的唯一方法是当我们收到“removeBlock”消息

            // The 'invalidate' list is used to inform the datanode the block 
            // should be deleted.  Items are removed from the invalidate list
            // upon giving instructions to the namenode.
            //` invalidate `列表用来通知datanode该数据块应该被删除。在向namenode发出指令后，元素会从无效列表中删除。
            // 删除有两种情况：1.超过最大副本数，2.删除文件时，会将文件的所有块放入invalidate列表中

            Vector invalidateSet = (Vector) recentInvalidateSets.get(cur.getName());
            if (invalidateSet == null) {
                invalidateSet = new Vector();
                recentInvalidateSets.put(cur.getName(), invalidateSet);
            }
            invalidateSet.add(b); //放入recentInvalidateSets {DN -> Vector{block1,block2,block3...}}
        }
    }

    /**
     * Modify (block-->datanode) map.  Possibly generate 
     * replication tasks, if the removed block is still valid.
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null || ! containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);

        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        if (dir.isValidBlock(block) && (containingNodes.size() < this.desiredReplication)) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
        }

        //
        // We've removed a block from a node, so it's definitely no longer
        // in "excess" there.
        // // excessReplicateMap冗余副本 移除多余的副本
        TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(node.getName());
        if (excessBlocks != null) {
            excessBlocks.remove(block);
            if (excessBlocks.size() == 0) {
                excessReplicateMap.remove(node.getName());
            }
        }
    }

    /**
     * The given node is reporting that it received a certain block.
     */
    public synchronized void blockReceived(Block block, UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Got blockReceived message from node " + name + ", but there is no info for " + name);
        }
        //
        // Modify the blocks->datanode map
        //
        addStoredBlock(block, node);

        //
        // Supplement node's blockreport
        //
        LOG.info("******** .DN.blocks "+node+"保存块信息, block:" +block);
        node.addBlock(block);
    }

    /**
     * Total raw bytes
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * Total non-used raw bytes
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    /**
     */
    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo results[] = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by the Namenode system, to see
    // if there is any work for a given datanode.
    //
    /////////////////////////////////////////////////////////

    /**
     * Check if there are any recently-deleted blocks a datanode should remove.
     */
    public synchronized Block[] blocksToInvalidate(UTF8 sender) {
        Vector invalidateSet = (Vector) recentInvalidateSets.remove(sender);
        if (invalidateSet != null) {
            return (Block[]) invalidateSet.toArray(new Block[invalidateSet.size()]);
        } else {
            return null;
        }
    }

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     *
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeInfo objs, identifying the
     *     target sequence for the Block at the appropriate index.
     *
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode, int xmitsInProgress) {
        synchronized (neededReplications) {
            Object results[] = null;
	    int scheduledXfers = 0;
            //
            if (neededReplications.size() > 0) {
                //
                // Go through all blocks that need replications.  See if any
                // are present at the current node.  If so, ask the node to
                // replicate them.
                //
                Vector replicateBlocks = new Vector();
                Vector replicateTargetSets = new Vector();
                for (Iterator it = neededReplications.iterator(); it.hasNext(); ) {
                    //
                    // We can only reply with 'maxXfers' or fewer blocks
                    //
                    if (scheduledXfers >= this.maxReplicationStreams - xmitsInProgress) {
                        break;
                    }

                    Block block = (Block) it.next();
                    if (! dir.isValidBlock(block)) {
                        it.remove();
                    } else {
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block); //找到待复制块的DN
                        if (containingNodes.contains(srcNode)) {  // 如果这个DN上有这个块  ,就选择 this.desiredReplication (3) - containingNodes.size() ,  this.maxReplicationStreams - xmitsInProgress  ,最小创建流
                            DatanodeInfo targets[] = chooseTargets(Math.min(this.desiredReplication - containingNodes.size(), this.maxReplicationStreams - xmitsInProgress), containingNodes);
                            if (targets.length > 0) {    // 假设这里传输的是两个目标DN
                                // Build items to return
                                replicateBlocks.add(block);
                                replicateTargetSets.add(targets);
				scheduledXfers += targets.length;
                            }
                        }
                    }
                }

                //
                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.  
                // (REMIND - mjc - this timer is not yet implemented.)
                //
                if (replicateBlocks.size() > 0) {
                    int i = 0;
                    for (Iterator it = replicateBlocks.iterator(); it.hasNext(); i++) {
                        Block block = (Block) it.next();
                        DatanodeInfo targets[] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        if (containingNodes.size() + targets.length >= this.desiredReplication) {
                            neededReplications.remove(block);  // 需要复制的集合移除 ,已经达到目标复制数(pendingReplications 已经添加了待复制的DN)
                            pendingReplications.add(block);  // 等待复制的集合添加 ,包括 已选择的target DN
                        }

			LOG.info("Pending transfer (block " + block.getBlockName() + ") from " + srcNode.getName() + " to " + targets.length + " destinations");
                    }

                    //
                    // Build returned objects from above lists
                    //
                    DatanodeInfo targetMatrix[][] = new DatanodeInfo[replicateTargetSets.size()][];
                    for (i = 0; i < targetMatrix.length; i++) {
                        targetMatrix[i] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                    }
                    // 返回这个节点待复制块 以及对应的 目标DN

                    results = new Object[2];
                    results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
                    results[1]  = targetMatrix;
                }
            }
            return results;
        }
    }

    /**
     * Get a certain number of targets, if possible.
     * If not, return as many as we can.
     * @param desiredReplicates number of duplicates wanted.
     * @param forbiddenNodes of DatanodeInfo instances that should not be
     * considered targets.
     * @return array of DatanodeInfo instances uses as targets.
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet forbiddenNodes) {
        TreeSet alreadyChosen = new TreeSet();
        Vector targets = new Vector();

        for (int i = 0; i < desiredReplicates; i++) {
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen);
            if (target != null) {
                targets.add(target);
                alreadyChosen.add(target);
            } else {
                break; // calling chooseTarget again won't help
            }
        }
        return (DatanodeInfo[]) targets.toArray(new DatanodeInfo[targets.size()]);
    }

    /**
     * Choose a target from available machines, excepting the
     * given ones.
     *
     * Right now it chooses randomly from available boxes.  In future could 
     * choose according to capacity and load-balancing needs (or even 
     * network-topology, to avoid inter-switch traffic).
     * @param forbidden1 DatanodeInfo targets not allowed, null allowed.
     * @param forbidden2 DatanodeInfo targets not allowed, null allowed.
     * @return DatanodeInfo instance to use or null if something went wrong
     * (a log message is emitted if null is returned).
     */
    DatanodeInfo chooseTarget(TreeSet forbidden1, TreeSet forbidden2) {
        //
        // Check if there are any available targets at all
        //
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOG.warning("While choosing target, totalMachines is " + totalMachines);
            return null;
        }

        TreeSet forbiddenMachines = new TreeSet();
        //
        // In addition to already-chosen datanode/port pairs, we want to avoid
        // already-chosen machinenames.  (There can be multiple datanodes per
        // machine.)  We might relax this requirement in the future, though. (Maybe
        // so that at least one replicate is off the machine.)
        //
        UTF8 hostOrHostAndPort = null;
        if (forbidden1 != null) {
          // add name [and host] of all elements in forbidden1 to forbiddenMachines
            for (Iterator it = forbidden1.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                if (allowSameHostTargets) {
                  hostOrHostAndPort = cur.getName(); // forbid same host:port
                } else {
                  hostOrHostAndPort = cur.getHost(); // forbid same host
                }
                forbiddenMachines.add(hostOrHostAndPort);
            }
        }
        if (forbidden2 != null) {
          // add name [and host] of all elements in forbidden2 to forbiddenMachines
            for (Iterator it = forbidden2.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
              if (allowSameHostTargets) {
                hostOrHostAndPort = cur.getName(); // forbid same host:port
              } else {
                hostOrHostAndPort = cur.getHost(); // forbid same host
              }
              forbiddenMachines.add(hostOrHostAndPort);
            }
        }

        //
        // Now build list of machines we can actually choose from
        //
        long totalRemaining = 0;
        Vector targetList = new Vector();
        for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeInfo node = (DatanodeInfo) it.next();
            if (allowSameHostTargets) {
                hostOrHostAndPort = node.getName(); // match host:port
            } else {
                hostOrHostAndPort = node.getHost(); // match host
            }
            if (! forbiddenMachines.contains(hostOrHostAndPort)) {
                targetList.add(node);
                totalRemaining += node.getRemaining();
            }
        }

        //
        // Now pick one
        //
        if (targetList.size() == 0) {
            new Exception(Thread.currentThread().getName()+" No targets in chooseTarget()").printStackTrace();
            LOG.warning("Zero targets found, forbidden1.size=" +
                ( forbidden1 != null ? forbidden1.size() : 0 ) +
                " allowSameHostTargets=" + allowSameHostTargets +
                " forbidden2.size()=" +
                ( forbidden2 != null ? forbidden2.size() : 0 ));
            return null;
        } else if (! this.useAvailability) {
            int target = r.nextInt(targetList.size());
            return (DatanodeInfo) targetList.elementAt(target);
        } else {
            // Choose node according to target capacity
            double target = r.nextDouble() * totalRemaining;

            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                target -= node.getRemaining();
                if (target <= 0) {
                    return node;
                }
            }

            LOG.warning("Impossible state.  When trying to choose target node, could not find any.  This may indicate that datanode capacities are being updated during datanode selection.  Anyway, now returning an arbitrary target to recover...");
            return (DatanodeInfo) targetList.elementAt(r.nextInt(targetList.size()));
        }
    }
}
