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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the Router using multiple connections.
 * It does NUM_TESTS * NUM_TESTS * NUM_TESTS tests.
 */
public class TestRouterConnectionMultiple {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterConnectionMultiple.class);

  private static final int NUM_NAMESERVICES = 1;
  private static final int NUM_TESTS = 20;

  /** Cluster with Namenodes and Routers. */
  private MiniRouterDFSCluster cluster;
  /** Executor for running tests. */
  private ExecutorService service;

  private FileSystem superFs;


  @Before
  public void setup() throws Exception {

    cluster = new MiniRouterDFSCluster(false, NUM_NAMESERVICES);
    cluster.startCluster();

    cluster.startRouters();
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    cluster.installMockLocations();

    // Reduce permissions without removing checks
    NameNode nn = cluster.getNamenode("ns0", null).getNamenode();
    int nnPort = nn.getNameNodeAddress().getPort();
    String username = UserGroupInformation.getLoginUser().getUserName();
    superFs = getFileSystem(nnPort, username);
    superFs.setPermission(new Path("/"), new FsPermission("777"));

    service = Executors.newFixedThreadPool(NUM_TESTS);
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      try {
        cluster.shutdown();
      } catch (Exception e) {
        LOG.error("Cannot stop the cluster: {}", e.getMessage());
      }
    }
    if (service != null) {
      service.shutdown();
    }
  }

  @Test
  public void test0SingleRouterSingleConnection() throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    int rpcPort = routerContext.getRpcPort();

    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_TESTS; i++) {
      FileSystem fs = getFileSystem(rpcPort, "user");
      Callable<Boolean> task = getTestFileSystemTask(fs, i);
      tasks.add(task);
    }

    for (int i = 0; i < NUM_TESTS; i++) {
      long t = execute(tasks);
      LOG.info("Time to run: {} ms", t);
    }
  }

  @Test
  public void test1SingleRouterMultipleConnection() throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    Configuration routerConf = routerContext.getConf();
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_MULTIPLE,
        true);
    int rpcPort = routerContext.getRpcPort();

    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_TESTS; i++) {
      FileSystem fs = getFileSystem(rpcPort, "user");
      Callable<Boolean> task = getTestFileSystemTask(fs, i);
      tasks.add(task);
    }

    for (int i = 0; i < NUM_TESTS; i++) {
      long t = execute(tasks);
      LOG.info("Time to run: {} ms", t);
    }
  }

  @Test
  public void test2SingleRouterMultipleUsers() throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    int rpcPort = routerContext.getRpcPort();

    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_TESTS; i++) {
      FileSystem fs = getFileSystem(rpcPort, "user-" + i);
      Callable<Boolean> task = getTestFileSystemTask(fs, i);
      tasks.add(task);
    }

    for (int i = 0; i < NUM_TESTS; i++) {
      long t = execute(tasks);
      LOG.info("Time to run: {} ms", t);
    }
  }

  @Test
  public void test3MultipleRoutersSingleConnection() throws Exception {

    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < NUM_TESTS; i++) {
      RouterContext routerContext = cluster.getRandomRouter();
      int rpcPort = routerContext.getRpcPort();

      FileSystem fs = getFileSystem(rpcPort, "user");
      Callable<Boolean> task = getTestFileSystemTask(fs, i);
      tasks.add(task);
    }

    for (int i = 0; i < NUM_TESTS; i++) {
      long t = execute(tasks);
      LOG.info("Time to run: {} ms", t);
    }
  }

  private static Callable<Boolean> getTestFileSystemTask(
      FileSystem fs, final int id) {
    return () -> {
      for (int j = 0; j < NUM_TESTS; j++) {
        Path dir = new Path("/test/dir-" + id + "-" + j);
        Path file = new Path(dir, "file" + j + ".txt");
        FSDataOutputStream os = fs.create(file);
        os.close();

        FileStatus dirStatus = fs.getFileStatus(dir);
        assertTrue(dirStatus.isDirectory());

        FileStatus fileStatus = fs.getFileStatus(file);
        assertTrue(fileStatus.isFile());
        assertEquals(0, fileStatus.getLen());

        assertTrue(fs.delete(dir, true));
      }

      return true;
    };
  }

  private static FileSystem getFileSystem(int rpcPort, String username)
      throws Exception {

    UserGroupInformation user = UserGroupInformation.createUserForTesting(
        username, new String[]{"group"});

    Configuration conf = new HdfsConfiguration();
    URI uri = URI.create("hdfs://localhost:" + rpcPort);
    FileSystem fs = DistributedFileSystem.get(uri, conf, user.getUserName());

    // Establish the first connection
    FileStatus[] files = fs.listStatus(new Path("/"));
    assertTrue("Files: " + Arrays.deepToString(files),
        files.length <= NUM_NAMESERVICES);

    return fs;
  }

  private long execute(final List<Callable<Boolean>> tasks)
      throws Exception {
    long t0 = Time.monotonicNow();
    AtomicInteger success = new AtomicInteger(0);
    service.invokeAll(tasks).forEach(task -> {
      try {
        if (task.get()) {
          success.incrementAndGet();
        }
      } catch (Exception e) {
        LOG.error("Cannot get result: {}", e.getMessage());
      }
    });
    long t = Time.monotonicNow() - t0;
    assertEquals(tasks.size(), success.get());
    return t;
  }
}