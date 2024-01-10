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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.service.Service.STATE;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS;
import static org.junit.Assert.assertEquals;

/**
 * The the safe mode for the {@link Router} controlled by
 *
 */
public class TestRouterWithoutDFSCluster {
  private static Configuration conf;

  @BeforeClass
  public static void create() throws IOException {
    // Basic configuration without the state store
    conf = new Configuration();
    // 1 sec cache refresh
    conf.setInt(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, 1);
    // Mock resolver classes
//    conf.setClass(RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
//        MockResolver.class, ActiveNamenodeResolver.class);
//    conf.setClass(RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
//        MockResolver.class, FileSubclusterResolver.class);

    conf.setClass(FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class, ActiveNamenodeResolver.class);
    conf.setClass(FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MultipleDestinationMountTableResolver.class, FileSubclusterResolver.class);

    // Bind to any available port
//    conf.set(RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
//    conf.set(RBFConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY, "0.0.0.0");


    // Simulate a co-located NN
//    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns0");
//    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + "ns0");
//    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + "ns0",
//        "127.0.0.1:0" + 0);
//    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY + "." + "ns0",
//        "127.0.0.1:" + 0);
//    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY + "." + "ns0",
//        "0.0.0.0");
  }

  private static void testRouterStartup(Configuration routerConfig)
      throws InterruptedException, IOException {
    Router router = new Router();
    assertEquals(STATE.NOTINITED, router.getServiceState());
    assertEquals(RouterServiceState.UNINITIALIZED, router.getRouterState());

    routerConfig.set("hadoop.zk.address","127.0.0.1:2181");
    routerConfig.set("ipc.client.connection.maxidletime","86400000");
    routerConfig.set("dfs.federation.router.connection.pool.clean.ms","86400000");
    routerConfig.set("dfs.federation.router.connection.clean.ms","86400000");
    routerConfig.set("dfs.federation.router.connection.min-active-ratio","0");

    routerConfig.setClass("dfs.federation.router.store.driver.class", StateStoreZooKeeperImpl.class, StateStoreDriver.class);
    routerConfig.set("zk-dt-secret-manager.zkAuthType","none");
    routerConfig.set("dfs.federation.router.store.driver.zk.parent-path","/hdfs-federation");
    routerConfig.set("zk-dt-secret-manager.zkConnectionString","127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
    routerConfig.set("zk-dt-secret-manager.znodeWorkingPath","rbf-zkdtsm");
    routerConfig.set("dfs.federation.router.store.router.expiration","60000");
    routerConfig.set("dfs.federation.router.store.membership.expiration","60000");
    routerConfig.set("dfs.federation.router.store.router.expiration.deletion","60000");
    routerConfig.set("dfs.federation.router.store.membership.expiration.deletion","60000");
    routerConfig.set("dfs.federation.router.connection.pool-size","5");

    routerConfig.set("dfs.federation.router.safemode.enable","false");

    routerConfig.set("dfs.federation.router.keytab.file","/Users/temp/zhangxiping.keytab");
    routerConfig.set("dfs.federation.router.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
    routerConfig.set("hadoop.security.authorization","true");
    routerConfig.set("hadoop.security.authentication","kerberos");
    routerConfig.set("dfs.block.access.token.enable","true");
    routerConfig.set("delegation-token.max-lifetime.sec","60");
    routerConfig.set("delegation-token.max-lifetime.sec.renew-interval.sec","10");
    routerConfig.set("delegation-token.removal-scan-interval.sec","3");
    routerConfig.set("delegation-token.update-interval.sec","60");


    router.init(routerConfig);
    if (routerConfig.getBoolean(
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
      assertEquals(RouterServiceState.SAFEMODE, router.getRouterState());
    } else {
      assertEquals(RouterServiceState.INITIALIZING, router.getRouterState());
    }
    assertEquals(STATE.INITED, router.getServiceState());
    router.start();
    if (routerConfig.getBoolean(
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE,
        RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE_DEFAULT)) {
      assertEquals(RouterServiceState.SAFEMODE, router.getRouterState());
    } else {
      assertEquals(RouterServiceState.RUNNING, router.getRouterState());
    }
  }



  static Configuration setRConf(Configuration conf, int index) {
    setLogLevel();
    System.setProperty("hadoop.log.file",index+"router.log");
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, 10);
    conf.set(DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:4025" + index);
    conf.set(DFS_ROUTER_RPC_BIND_HOST_KEY, "127.0.0.1");

    conf.set(DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:4026" + index);
    conf.set(DFS_ROUTER_ADMIN_BIND_HOST_KEY, "127.0.0.1");

    conf.set(DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:5071" + index);
    conf.set(DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_HTTP_BIND_HOST_KEY, "127.0.0.1");

    conf.set(DFS_ROUTER_DEFAULT_NAMESERVICE, "");
    conf.setLong(DFS_ROUTER_HEARTBEAT_INTERVAL_MS, 5000);
    conf.setLong(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, 5000);


    conf.addResource(new Path("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml"));

    conf.set("dfs.federation.router.default.nameserviceId", "minidfs-ns");
    conf.set("dfs.federation.router.monitor.namenode", "minidfs-ns.nn1,minidfs-ns.nn2");
    return conf;

  }

  static void setLogLevel(){
    System.setProperty("hadoop.metrics.logger","INFO,RFAMETRIC");
    System.setProperty("hadoop.root.logger","INFO,console");
    System.setProperty("hadoop.security.logger","INFO,DRFAS");
    System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%d{ISO8601} : %m%n");
  }

  @Test
  public void testZK() throws Exception {
    System.setProperty("zookeeper.4lw.commands.whitelist","*");
    TestingServer zkServer = new TestingServer(new InstanceSpec(new File("/data/zkData"), 2181, -1, -1, true, -1),true);
    zkServer.start();
    System.in.read();
  }

  //静态资源位置的获取
//  private HttpServer2(final HttpServer2.Builder b) throws IOException {
//    final String appDir = getWebAppsPath(b.name);

  @Test
  public void testZKCluster() throws Exception {
    System.setProperty("zookeeper.4lw.commands.whitelist","*");
    System.setProperty("zookeeper.admin.serverPort","9181");

    ImmutableList.Builder<InstanceSpec> builder = ImmutableList.builder();
    for ( int i = 0; i < 3; ++i )
    {
      builder.add(new InstanceSpec(null, 2181+i, -1, -1, true, i));
    }
    TestingCluster zkCluster = new TestingCluster(builder.build());
    zkCluster.start();
    System.out.println("**************** "+ zkCluster.getConnectString());
    System.in.read();
  }

  @Test
  public void testRouter0() throws InterruptedException, IOException {
    Configuration conf = new Configuration();
    // Run with all services

    testRouterStartup(setRConf(conf,0));
    System.in.read();
  }

  @Test
  public void testRouter1() throws InterruptedException, IOException {
    Configuration conf = new Configuration();
    // Run with all services

    testRouterStartup(setRConf(conf,1));
    System.in.read();
  }

  @Test
  public void testRouter2() throws InterruptedException, IOException {

    // Run with all services
    testRouterStartup(setRConf(conf,2));
    System.in.read();
  }

  @Test
  public void testRouter3() throws InterruptedException, IOException {

    // Run with all services
    testRouterStartup(setRConf(conf,3));
    System.in.read();
  }

  @Test
  public void testRouter4() throws InterruptedException, IOException {

    // Run with all services
    testRouterStartup(setRConf(conf,4));
    System.in.read();
  }

  @Test
  public void testRouter5() throws InterruptedException, IOException {

    // Run with all services
    testRouterStartup(setRConf(conf,5));
    System.in.read();
  }

  @Test
  public void testRouter6() throws InterruptedException, IOException {

    // Run with all services
    testRouterStartup(setRConf(conf,6));
    System.in.read();
  }

  @Test
  public  void test03(){
    List<String> list1=new ArrayList();
    list1.add("A");
    list1.add("B");
    list1.add("C");
    list1.add("D");
    System.out.println(list1);

    list1.add(2,"小黑");
    System.out.println(list1);
    list1.remove(2);
    System.out.println(list1);
    list1.set(3,"小白");
    System.out.println(list1);
    System.out.println(list1.get(2));
    list1.add("B");
    System.out.println(list1);
    //返回元素从左第一次出现的位置
    System.out.println(list1.indexOf("B"));
    //返回元素从右往左第一次出现的位置
    System.out.println(list1.lastIndexOf("B"));
  }
}
