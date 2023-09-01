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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 * Tests covering the functionality of RouterRPCServer with
 * multi nameServices.
 *
 *
 * 出现  java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$POSIX.stat(Ljava/lang/String;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat
 *  C:\Windows\System32   加入  hadoop-common 编译出来的 hadoop.dll
 *
 *
 */
public class TestRouterWithDFSCluster {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestRouterWithDFSCluster.class);

    private static final int NUM_SUBCLUSTERS = 2;
    // We need at least 6 DNs to test Erasure Coding with RS-6-3-64k
    private static final int NUM_DNS = 6;


    private static final Comparator<ErasureCodingPolicyInfo> EC_POLICY_CMP =
            new Comparator<ErasureCodingPolicyInfo>() {
                public int compare(
                        ErasureCodingPolicyInfo ec0,
                        ErasureCodingPolicyInfo ec1) {
                    String name0 = ec0.getPolicy().getName();
                    String name1 = ec1.getPolicy().getName();
                    return name0.compareTo(name1);
                }
            };

    /** Federated HDFS cluster. */
    private static MiniRouterDFSCluster cluster;

    /** Random Router for this federated cluster. */
    private MiniRouterDFSCluster.RouterContext router;

    /** Random nameservice in the federated cluster.  */
    private String ns;
    /** First namenode in the nameservice. */
    private MiniRouterDFSCluster.NamenodeContext namenode;

    /** Client interface to the Router. */
    private ClientProtocol routerProtocol;
    /** Client interface to the Namenode. */
    private ClientProtocol nnProtocol;

    /** NameNodeProtocol interface to the Router. */
    private NamenodeProtocol routerNamenodeProtocol;
    /** NameNodeProtocol interface to the Namenode. */
    private NamenodeProtocol nnNamenodeProtocol;

    /** Filesystem interface to the Router. */
    private FileSystem routerFS;
    /** Filesystem interface to the Namenode. */
    private FileSystem nnFS;

    /** File in the Router. */
    private String routerFile;
    /** File in the Namenode. */
    private String nnFile;

    static void setLogLevel(){
        System.setProperty("hadoop.metrics.logger","INFO,RFAMETRIC");
        System.setProperty("hadoop.root.logger","INFO,console");
        System.setProperty("hadoop.security.logger","INFO,DRFAS");
        System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%d{ISO8601} : %m%n");
    }

    @Test
    public void testZK() throws Exception {
        TestingServer zkServer = new TestingServer(new InstanceSpec(new File("/data/zkData"), 2181, -1, -1, true, -1),true);
        zkServer.start();
        System.in.read();
    }

    @Test
    public void startRouter() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","router.log");
        Configuration namenodeConf = new Configuration();
        namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
                true);
        // It's very easy to become overloaded for some specific dn in this small
        // cluster, which will cause the EC file block allocation failure. To avoid
        // this issue, we disable considerLoad option.
        namenodeConf.setBoolean(DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
        namenodeConf.set("ipc.client.idlethreshold","0");
        namenodeConf.set("fs.trash.interval","1440");
        namenodeConf.set("fs.trash.checkpoint.interval","1440");
        cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
        cluster.setNumDatanodesPerNameservice(NUM_DNS);
        cluster.addNamenodeOverrides(namenodeConf);
        cluster.setIndependentDNs();

        // Start NNs and DNs and wait until ready
        cluster.startCluster();

        // Start routers with only an RPC service
        Configuration routerConf = new RouterConfigBuilder()
                .enableLocalHeartbeat(true)
                .stateStore()
                .heartbeat()
                .admin()
                .rpc()
                .http(true)
                .refreshCache()
                .build();
        // We decrease the DN cache times to make the test faster
        routerConf.setTimeDuration(
                RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
        routerConf.set("hadoop.zk.address","127.0.0.1:2181");

//        routerConf.set("ipc.client.connection.maxidletime","86400000");
//        routerConf.set("dfs.federation.router.connection.pool.clean.ms","86400000");
//        routerConf.set("dfs.federation.router.connection.clean.ms","86400000");
        routerConf.set("dfs.federation.router.connection.min-active-ratio","0");


        routerConf.set("dfs.federation.router.file.resolver.client.class","org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver");
        routerConf.setClass("dfs.federation.router.store.driver.class", StateStoreZooKeeperImpl.class, StateStoreDriver.class);
        routerConf.set("dfs.federation.router.store.driver.zk.parent-path","/hdfs-federation");
        routerConf.set("zk-dt-secret-manager.zkConnectionString","127.0.0.1:2181");
        routerConf.set("zk-dt-secret-manager.znodeWorkingPath","rbf-zkdtsm");
        routerConf.set("dfs.federation.router.store.router.expiration","60000");
        routerConf.set("dfs.federation.router.store.membership.expiration","60000");
        routerConf.set("dfs.federation.router.store.router.expiration.deletion","60000");
        routerConf.set("dfs.federation.router.store.membership.expiration.deletion","60000");
        routerConf.set("dfs.federation.router.connection.pool-size","5");

        cluster.addRouterOverrides(routerConf);
        cluster.startMYRouters();

        // Register and verify all NNs with all routers
        cluster.registerNamenodes();
        cluster.waitNamenodeRegistration();

        // Create mock locations
//        cluster.installLocations();

        // Delete all files via the NNs and verify
        cluster.deleteAllFiles();

        // Create test fixtures on NN
        cluster.createTestDirectoriesNamenode();

        // Wait to ensure NN has fully created its test directories
        Thread.sleep(100);
        // Random router for this test
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/hdfs-site.xml")));
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/yarn-site.xml")));
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/hdfs-site.xml")));
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/yarn-site.xml")));
        
        cluster.getRouters().get(0).getConf().writeXml(new FileOutputStream(new File("/project/neproject/3.3.0/ne-hadoop/hadoop-tools/hadoop-distcp/target/classes/core-site.xml")));
        cluster.getRouters().get(0).getAdminClient();
        System.in.read();

    }

    @AfterClass
    public static void tearDown() {
        cluster.shutdown();
    }

    protected MiniRouterDFSCluster getCluster() {
        return TestRouterWithDFSCluster.cluster;
    }

    protected MiniRouterDFSCluster.RouterContext getRouterContext() {
        return this.router;
    }

    protected void setRouter(MiniRouterDFSCluster.RouterContext r)
            throws IOException, URISyntaxException {
        this.router = r;
        this.routerProtocol = r.getClient().getNamenode();
        this.routerFS = r.getFileSystem();
        this.routerNamenodeProtocol = NameNodeProxies.createProxy(router.getConf(),
                router.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
    }

    protected FileSystem getRouterFileSystem() {
        return this.routerFS;
    }

    protected FileSystem getNamenodeFileSystem() {
        return this.nnFS;
    }

    protected ClientProtocol getRouterProtocol() {
        return this.routerProtocol;
    }

    protected ClientProtocol getNamenodeProtocol() {
        return this.nnProtocol;
    }

    protected MiniRouterDFSCluster.NamenodeContext getNamenode() {
        return this.namenode;
    }

    protected void setNamenodeFile(String filename) {
        this.nnFile = filename;
    }

    protected String getNamenodeFile() {
        return this.nnFile;
    }

    protected void setRouterFile(String filename) {
        this.routerFile = filename;
    }

    protected String getRouterFile() {
        return this.routerFile;
    }

    protected void setNamenode(MiniRouterDFSCluster.NamenodeContext nn)
            throws IOException, URISyntaxException {
        this.namenode = nn;
        this.nnProtocol = nn.getClient().getNamenode();
        this.nnFS = nn.getFileSystem();

        // Namenode from the default namespace
        String ns0 = cluster.getNameservices().get(0);
        MiniRouterDFSCluster.NamenodeContext nn0 = cluster.getNamenode(ns0, null);
        this.nnNamenodeProtocol = NameNodeProxies.createProxy(nn0.getConf(),
                nn0.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
    }

    protected String getNs() {
        return this.ns;
    }

    protected void setNs(String nameservice) {
        this.ns = nameservice;
    }

}