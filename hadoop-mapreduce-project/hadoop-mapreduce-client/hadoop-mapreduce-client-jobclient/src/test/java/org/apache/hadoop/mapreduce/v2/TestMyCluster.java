package org.apache.hadoop.mapreduce.v2;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;


/**
 *
 * datanode ssl 配置 https://blog.csdn.net/anxia5150/article/details/101966285
 *
 * webapps/hdfs  not found   , 在D:\project\neproject\3\ne-hadoop\  执行  mvn clean package -Dmaven.javadoc.skip=true -Pdist -DskipTests
 *
 * TestAclsEndToEnd  报 符号找不到   运行一下这里面的测试用例
 *
 * import org.apache.hadoop.minikdc.KerberosSecurityTestcase;    minikdc 找不到  运行一下  TestMiniKdc.testKeytabGen
 *
 * idea  generated-sources    不生效   右侧 maven reload
 *
 * java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$POSIX.stat(Ljava/lang/String;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
 * Edit configuration  -》  PATH=D:\project\neproject\3.3.0\ne-hadoop\hadoop-common-project\hadoop-common\target\bin;%PATH%
 *
 * for  mac
 * Native_lib    或     Secure IO is not possible without native code extensions
 * 拷贝  hadoop-dist  target/lib/native    /Users/zhangxiping/Library/Java/Extensions
 *
 */

public class TestMyCluster {
    private MiniKMS miniKMS;
    private MiniKdc kdc;
    private File workDir;
    static String projectPath = new File("").getAbsolutePath();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();
    static String NAMESERVICE = "minidfs-ns";
    static String NN1 = "nn1";
    static String NN2 = "nn2";
    //static String tmpDir = "D:\\project\\myproject\\hadoop3.3.0\\hadoop-mapreduce-project\\hadoop-mapreduce-client\\hadoop-mapreduce-client-jobclient\\target\\test-dir\\dfs";

    private static String prepareNMDirs(String dirType, int numDirs,int index) {
        File []dirs = new File[numDirs];
        String dirsString = "";
        String path ="";

        for (int i = 0; i < numDirs; i++) {
            dirs[i]= new File("/Users/temp/1606271550694", "test"
                + "-" + dirType + "Dir-nm-" + index + "_" + i);
            dirs[i].mkdirs();
            String delimiter = (i > 0) ? "," : "";
            dirsString = dirsString.concat(delimiter + dirs[i].getAbsolutePath());
        }
        return dirsString;
    }

    static Configuration getNMConf(int port,int index) throws IOException {

        Configuration config = new YarnConfiguration();
        // create nm-local-dirs and configure them for the nodemanager
        String localDirsString = prepareNMDirs("local", 1,index);
        config.set(YarnConfiguration.NM_LOCAL_DIRS, localDirsString);
        // create nm-log-dirs and configure them for the nodemanager
        String logDirsString = prepareNMDirs("log", 1,index);
        config.set(YarnConfiguration.NM_LOG_DIRS, logDirsString);

        config.setInt(YarnConfiguration.NM_PMEM_MB, config.getInt(
            YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB,
            YarnConfiguration.DEFAULT_YARN_MINICLUSTER_NM_PMEM_MB));
        config.set(YarnConfiguration.NM_ADDRESS,"0.0.0.0:2594"+index);
        config.set(YarnConfiguration.NM_WEBAPP_ADDRESS,
            "0.0.0.0:804"+index);
        config.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
            "0.0.0.0:0");
        config.set(YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
            "0.0.0.0:0");

        config.setBoolean(
            YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION, false);
        // Disable resource checks by default
        if (!config.getBoolean(
            YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
            YarnConfiguration.
                DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING)) {
            config.setBoolean(
                YarnConfiguration.NM_CONTAINER_MONITOR_ENABLED, false);
            config.setLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS, 0);
        }
        config.setInt("mapreduce.shuffle.port", port);
        //        config.set("yarn.nodemanager.resource.memory-mb", "47884971");
        //        config.set("yarn.nodemanager.resource.cpu-vcores", "7671");

        config.set("yarn.nodemanager.resource.memory-mb", "20480");
        config.set("yarn.nodemanager.resource.cpu-vcores", "16");

        config.set("yarn.log.server.url", "http://127.0.0.1:19888/jobhistory/logs");
        config.unset("dfs.http.policy");
        config.set("yarn.node-labels.enabled","true");
        config.set("yarn.nodemanager.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        config.set("yarn.nodemanager.keytab","/Users/temp/zhangxiping.keytab");

        return config;
    }

    static Configuration getDNConf(int i) throws IOException {
        Configuration conf = new HdfsConfiguration();
        conf.set(DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");
        conf.setLong(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 0);
        conf.set("dfs.http.policy","HTTP_ONLY");
        conf.set("ignore.secure.ports.for.testing","true");
        conf.set("dfs.datanode.address","0.0.0.0:"+6100+i);
        conf.set(" dfs.datanode.ipc.address","0.0.0.0:"+987+i);
        conf.set("dfs.datanode.http.address","0.0.0.0:"+6101+i);
        conf.set("dfs.datanode.https.address","0.0.0.0:"+6102+i);
        String base_dir = projectPath+"/target/test-dir/dfs";
        // String base_dir = tmpDir;
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < 2; ++j) {
            File dir;
            dir = new File(base_dir, "data/data" + (2 * i + 1 + j));
            dir.mkdir();
            sb.append((j > 0 ? "," : "") + "[" +
                StorageType.DEFAULT  +
                "]" + fileAsURI(dir));
        }
        conf.set(DFS_DATANODE_DATA_DIR_KEY, sb.toString());
        System.out.println("Starting DataNode " + i + " with "
            + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
            + conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
        return conf;
    }

    static Configuration initHAConf(URI journalURI, Configuration conf,
                                    int basePort) {

        String address1 = "127.0.0.1:" + basePort;
        String address2 = "127.0.0.1:" + (basePort + 2);
        conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
            NAMESERVICE, NN1), address1);
        conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
            NAMESERVICE, NN2), address2);
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, NAMESERVICE);
        conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, NAMESERVICE),
            NN1 + "," + NN2);
        conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + NAMESERVICE,
            ConfiguredFailoverProxyProvider.class.getName());
        conf.set("fs.defaultFS", "hdfs://" + NAMESERVICE);

        return conf;
    }

    public static MiniDFSNNTopology simpleHATopologyWithBasePort(int basePort) {
        return new MiniDFSNNTopology()
            .addNameservice(new MiniDFSNNTopology.NSConf("minidfs-ns").addNN(
                new MiniDFSNNTopology.NNConf("nn1").setIpcPort(basePort)//.setHttpsPort(basePort + 11)
                    .setHttpPort(basePort + 1)).addNN(
                new MiniDFSNNTopology.NNConf("nn2").setIpcPort(basePort + 2)//.setHttpsPort(basePort + 12)
                    .setHttpPort(basePort + 3)));
    }

    static void clearClassPath(){
        File hdfs_site0 = new File(projectPath + "/target/classes/hdfs-site.xml");
        hdfs_site0.delete();
        File core_site = new File(projectPath + "/target/test-classes/core-site.xml");
        core_site.delete();
        File hdfs_site = new File(projectPath + "/target/test-classes/hdfs-site.xml");
        hdfs_site.delete();
        File mapRed_site = new File(projectPath + "/target/test-classes/mapred-site.xml");
        mapRed_site.delete();
        File yarn_site = new File(projectPath + "/target/test-classes/yarn-site.xml");
        yarn_site.delete();
    }

    static void clearRMClassPath(){
        File mapRed_site = new File(projectPath + "/target/test-classes/mapred-site.xml");
        mapRed_site.delete();
        File yarn_site = new File(projectPath + "/target/test-classes/yarn-site.xml");
        yarn_site.delete();
    }



    static void setHdfsCommonConf( Configuration conf){
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        conf.set("hadoop.rpc.protection","authentication");
        conf.set("hadoop.security.auth_to_local","RULE:[2:$1@$0](.*@EXAMPLE.COM)s/.*/zhangxiping/" +
            "\nRULE:[1:$1@$0](.*)s/.*/zhangxiping/"+  // 跟我们公司的认证环境有冲突
            "\nDEFAULT");

        //jn
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.kerberos.internal.spnego.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");


        // namenode
        conf.set("dfs.namenode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.namenode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");

        // web  authentication
//        conf.set("dfs.web.authentication.kerberos.principal","HTTP/127.0.0.1@EXAMPLE.COM");
//        conf.set("dfs.web.authentication.kerberos.keytab","/Users/temp/zhangxiping.keytab");
//        conf.set("dfs.namenode.kerberos.internal.spnego.principal","HTTP/127.0.0.1@EXAMPLE.COM");
//        conf.set("hadoop.http.authentication.type","kerberos");
//        conf.set("hadoop.http.authentication.kerberos.principal","HTTP/127.0.0.1@EXAMPLE.COM");
//        conf.set("hadoop.http.authentication.kerberos.keytab","/Users/temp/zhangxiping.keytab");

        conf.set("net.topology.script.file.name",projectPath + "/target/test-classes/topology_script.cmd");
        conf.set("net.topology.node.switch.mapping.impl","org.apache.hadoop.net.ScriptBasedMapping");
        conf.set("dfs.replication","3");
        conf.set("fs.trash.interval","5");
        conf.set("fs.trash.checkpoint.interval","5");
        conf.set("dfs.namenode.recycle.bin.enabled","true");
        conf.set("dfs.namenode.delegation.token.max-lifetime",7*24*60*60*100+"0");
        conf.set("dfs.namenode.delegation.token.renew-interval",24*60*60*100+"0");
        conf.set("dfs.block.access.token.enable","true");

        //mapreduce
        conf.set("yarn.app.mapreduce.am.log.level","info");
        conf.set("mapreduce.map.log.level","info");
        conf.set("mapreduce.reduce.log.level","info");
        //这样设置输出的日志格式utf-8
        conf.set("mapred.child.java.opts","-Dfile.encoding=UTF-8");
        conf.set("yarn.app.mapreduce.am.command-opts","-Dfile.encoding=UTF-8");
        conf.set("mapreduce.client.submit.file.replication","3");

        //kms
        conf.set("hadoop.security.key.provider.path","kms://http@localhost:16000/kms");
        conf.set("dfs.encryption.key.provider.uri","kms://http@localhost:16000/kms");

        // dfs.https.enable 不设置  DN 报错 Cannot start secure DataNode without configuring either privileged resources or SASL RPC data transf
        //datanode
        conf.set("dfs.https.enable","true");
        conf.set("ssl.server.keystore.location",projectPath + "/target/test-classes/keystore.jks");
        conf.set("dfs.data.transfer.protection","integrity");
        conf.set("dfs.datanode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.datanode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");

        //代理用户
        conf.set("hadoop.proxyuser.zhangxiping.users","*");
        conf.set("hadoop.proxyuser.zhangxiping.groups","*");
        conf.set("hadoop.proxyuser.zhangxiping.hosts","*");
    }

    static void setLogLevel(){
        System.setProperty("hadoop.metrics.logger","INFO,RFAMETRIC");
        System.setProperty("hadoop.root.logger","INFO,console");
        System.setProperty("hadoop.security.logger","INFO,DRFAS");
        System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%d{ISO8601} : %m%n");
    }
    static Configuration getRMConf(Configuration conf, int index){
        System.setProperty("java.security.krb5.conf",projectPath+"/target/test-classes/krb5.conf");
        //不设置会提示MRAppMaster 类找不到
        conf.setBoolean("yarn.minicluster.use-rpc", true);
        conf.setBoolean("yarn.is.minicluster", true);

        conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
        conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);

        conf.set("yarn.resourcemanager.cluster-id", "test");
        conf.set("yarn.resourcemanager.webapp.address.rm2","0.0.0.0:28088");
        conf.set("yarn.resourcemanager.webapp.address.rm1","0.0.0.0:18088");
        conf.set("yarn.resourcemanager.ha.rm-ids","rm1,rm2");
        conf.set("yarn.resourcemanager.address.rm2","0.0.0.0:28032");
        conf.set("yarn.resourcemanager.address.rm1","0.0.0.0:18032");
        conf.set("yarn.resourcemanager.scheduler.address.rm2","0.0.0.0:28030");
        conf.set("yarn.resourcemanager.scheduler.address.rm1","0.0.0.0:18030");
        conf.set("yarn.resourcemanager.ha.id","rm"+index);
        conf.set("yarn.resourcemanager.resource-tracker.address.rm2","0.0.0.0:28031");
        conf.set("yarn.resourcemanager.resource-tracker.address.rm1","0.0.0.0:18031");
        conf.set("yarn.resourcemanager.admin.address.rm2","127.0.0.1:28033");
        conf.set("yarn.resourcemanager.admin.address.rm1","127.0.0.1:18033");

        conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        conf.set("yarn.nodemanager.aux-services.mapreduce_shuffle.class","org.apache.hadoop.mapred.ShuffleHandler");

        conf.set("mapreduce.framework.name","yarn");

        conf.set("yarn.node-labels.enabled","true");
        conf.set("yarn.node-labels.fs-store.root-dir","/lable");
        conf.set("yarn.node-attribute.fs-store.root-dir","/node-attribute");

        conf.set("yarn.resourcemanager.recovery.enabled","true");
        conf.set("yarn.resourcemanager.store.class","org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore");
        conf.set("yarn.resourcemanager.fs.state-store.uri","/rmstore");

        conf.set("yarn.scheduler.capacity.resource-calculator","org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
        conf.set("yarn.resourcemanager.scheduler.class","org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");
        conf.set("yarn.scheduler.capacity.label-metrics.enable","true");
        conf.set("yarn.resourcemanager.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("yarn.resourcemanager.keytab","/Users/temp/zhangxiping.keytab");

        //conf.set("yarn.resourcemanager.scheduler.class","org.apache.hadoop.yarn.sls.scheduler.SLSCapacityScheduler");
        //conf.set("yarn.resourcemanager.webapp.address","127.0.0.1:8088");
        conf.set("yarn.log-aggregation-enable","true");
        conf.set("yarn.log-aggregation.file-formats","TFile");
        conf.set("yarn.log-aggregation.file-controller.TFile.class","org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController");
        conf.set("mapreduce.jobhistory.address","0.0.0.0:10021");
        conf.set("mapreduce.jobhistory.webapp.address","0.0.0.0:19888");
        conf.set("yarn.nodemanager.log-aggregation.queue-monitoring-interval-seconds","10");
        conf.unset("dfs.http.policy");
        conf.set("yarn.resourcemanager.zk-address", "127.0.0.1:2181");
        return conf;
    }
    //HA
    @Test
    public void testKDC() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","KDC.log");
        //先清理classpath配置文件
        clearClassPath();
        Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        workDir = folder.getRoot();
        Properties kConf = MiniKdc.createConf();
        kConf.setProperty("debug","true");
        kConf.setProperty("kdc.port","2222");
        kConf.setProperty("org.name","EXAMPLE");
        kConf.setProperty("org.domain","COM");
        kConf.setProperty("transport","TCP");
        kdc = new MiniKdc(kConf, workDir);
        kdc.start();

        conf.set("hadoop.security.authorization","true");
        conf.set("hadoop.security.authentication","kerberos");
        //conf.set("ipc.client.fallback-to-simple-auth-allowed","true");

        conf.set("hadoop.security.auth_to_local","RULE:[2:$1@$0](.*@EXAMPLE.COM)s/.*/zhangxiping/\n"+"DEFAULT");
        // NN 可能依赖环境变量里面的配置
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.kerberos.internal.spnego.principal","HTTP/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.qjournal.queued-edits.limit.mb","1");

        UserGroupInformation.setShouldRenewImmediatelyForTests(true);
        String [] principals = new String[]{"hdfs/127.0.0.1","zhangxiping/127.0.0.1","HTTP/127.0.0.1"};
        File keytab = new File("/Users/temp/zhangxiping.keytab");
        System.out.println("============="+keytab.getAbsolutePath());
        // window 默认会加载  C://windows/krb5.ini
        FileUtils.copyFile(new File(projectPath + "/target/test-classes/krb5.conf"), new File("C:\\windows\\krb5.ini"));
        FileUtils.copyFile(new File(projectPath + "/target/test-classes/keystore.jks"), new File("/Users/temp/keystore.jks"));
        FileUtils.copyFile(new File(projectPath + "/target/test-classes/truststore.jks"), new File("/Users/temp/truststore.jks"));

        kdc.createPrincipal(keytab, principals);
        conf.set("hadoop.http.authentication.simple.anonymous.allowed","true");
        conf.set("hadoop.http.filter.initializers","org.apache.hadoop.security.AuthenticationFilterInitializer");
        conf.set("hadoop.http.authentication.signature.secret.file","/Users/temp/hadoop-http-auth-signature-secret");

        conf.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/core-site.xml")));
        conf.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
        System.out.println("----------write success!!");
        System.in.read();
    }

    // java.lang.RuntimeException: Could not find kms-webapp/ dir in test classpath
    // 拷贝  D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-kms\target\test-classes\kms-webapp
    // 到  D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\target\test-classes
    //
    // 出现 keystore——old 找不到  使用超级管理员账户重启idea
    @Test
    public void testKMS() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","KMS.log");
        final String keystore;
        final String password;
        File kmsDir = new File(projectPath +"/target/test-classes/" );
        Configuration conf = new Configuration();
        conf.set("hadoop.kms.authentication.type", "simple");
        KeyStoreTestUtil.setupSSLConfig(kmsDir.getAbsolutePath(), projectPath +"/target/test-classes/",
            conf, false);
        keystore = kmsDir.getAbsolutePath() + "/serverKS.jks";
        password = "serverP";

        MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
        miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).setPort(16000).build();
        miniKMS.start();
        System.out.println("------------- KMS runing" );
        System.in.read();
    }
    //HA
    @Test
    public void testMiniQJM() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","JN.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();
        //DefaultMetricsSystem.setMiniClusterMode(true);
        //conf.set("ignore.secure.ports.for.testing","true");
        conf.set("hadoop.security.authorization","true");
        conf.set("hadoop.security.authentication","kerberos");
        conf.set("dfs.http.policy","HTTP_ONLY");
        conf.set("dfs.qjournal.queued-edits.limit.mb","1");
        conf.set("hadoop.security.auth_to_local","RULE:[2:$1@$0](.*@EXAMPLE.COM)s/.*/zhangxiping/\n"+"DEFAULT");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.kerberos.internal.spnego.principal","HTTP/127.0.0.1@EXAMPLE.COM");
        MiniJournalCluster journalCluster = new MiniJournalCluster.Builder(conf)
            .setRpcPorts(8470,8471,8472)
            .format(true)
            .build();
        journalCluster.waitActive();
        journalCluster.setNamenodeSharedEditsConf(NAMESERVICE);
        URI journalURI = journalCluster.getQuorumJournalURI(NAMESERVICE);
        System.out.println(journalURI.toString());
        System.in.read();
    }

    static Configuration setHdfsHAConf(Configuration conf,int index ,boolean format) throws IOException {
        conf.set("dfs.client.failover.proxy.provider.minidfs-ns","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.nameservices","minidfs-ns");
        conf.set("fs.defaultFS","hdfs://minidfs-ns");
        conf.set("dfs.nameservice.id","minidfs-ns");
        conf.set("dfs.ha.namenodes.minidfs-ns","nn1,nn2");
        conf.set("dfs.namenode.rpc-address.minidfs-ns.nn1","127.0.0.1:9020");
        conf.set("dfs.namenode.rpc-address.minidfs-ns.nn2","127.0.0.1:9030");
        conf.set("ipc.client.fallback-to-simple-auth-allowed","true");
        conf.set("dfs.namenode.http-address.minidfs-ns.nn1","127.0.0.1:50070");
        conf.set("dfs.namenode.http-address.minidfs-ns.nn2","127.0.0.1:50080");
        conf.set("dfs.namenode.shared.edits.dir", "qjournal://127.0.0.1:8470;127.0.0.1:8471;127.0.0.1:8472/minidfs-ns");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.keytab.file","/Users/temp/zhangxiping.keytab");
        conf.set("dfs.journalnode.kerberos.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.journalnode.kerberos.internal.spnego.principal","HTTP/127.0.0.1@EXAMPLE.COM");
        conf.set("dfs.ha.namenode.id","nn"+index);
        conf.set("dfs.namenode.name.dir","file:/"+projectPath.replaceAll("\\\\","/") +"/target/test-dir/dfs/name"+index);
        conf.set("dfs.namenode.checkpoint.dir","file:/"+projectPath.replaceAll("\\\\","/") +"/target/test-dir/dfs/namesecondary"+index);
        conf.set("ha.zookeeper.quorum","127.0.0.1:2181");
        conf.set("dfs.ha.automatic-failover.enabled","true");
        formatNameNode(conf,true,index);
        return conf;
    }

    static void formatNameNode(Configuration conf,boolean format,int index) throws IOException {
        Collection<URI> namespaceDirs = FSNamesystem.getNamespaceDirs(conf);
        if (format) {
            // delete the existing namespaces
            for (URI nameDirUri : namespaceDirs) {
                File nameDir = new File(nameDirUri);
                if (nameDir.exists() && !FileUtil.fullyDelete(nameDir)) {
                    throw new IOException("Could not fully delete " + nameDir);
                }
            }
            // delete the checkpoint directories, if they exist
            Collection<URI> checkpointDirs = Util.stringCollectionAsURIs(conf
                .getTrimmedStringCollection(DFS_NAMENODE_CHECKPOINT_DIR_KEY));
            for (URI checkpointDirUri : checkpointDirs) {
                File checkpointDir = new File(checkpointDirUri);
                if (checkpointDir.exists() && !FileUtil.fullyDelete(checkpointDir)) {
                    throw new IOException("Could not fully delete " + checkpointDir);
                }
            }
        }

        if (index == 1 && format) {
            HdfsServerConstants.StartupOption.FORMAT.setClusterId("minidfs-ns");
            DFSTestUtil.formatNameNode(conf);
        }
    }

    //NO HA
    @Test
    public void testNoHANN() throws Exception {
        clearRMClassPath();
        clearClassPath();
        setLogLevel();
        System.setProperty("hadoop.log.file","NN.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new HdfsConfiguration();
        setHdfsCommonConf(conf);
        conf.set("dfs.namenode.rpc-address","127.0.0.1:8020");
        conf.set("dfs.namenode.rpc-address","127.0.0.1:8020");
        conf.set("dfs.namenode.name.dir","file:/"+projectPath.replaceAll("\\\\","/") +"/target/test-dir/dfs/name");
        conf.set("dfs.namenode.checkpoint.dir","file:/"+projectPath.replaceAll("\\\\","/") +"/target/test-dir/dfs/namesecondary");
        DefaultMetricsSystem.setMiniClusterMode(true);
        formatNameNode(conf,true,1);
        NameNode nn =  NameNode.createNameNode(new String[] {}, conf);
        conf.set("fs.defaultFS","hdfs://localhost:8020");
        conf.unset("ipc.client.fallback-to-simple-auth-allowed");
        nn.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/core-site.xml")));
        nn.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
        nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/hdfs-site.xml")));
        nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
        nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/hdfs-site.xml")));
        nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
        System.out.println("*********************** 文件写入成功!");
        System.in.read();
    }

    @Test
    public void testNN1() throws Exception {
        clearRMClassPath();
        setLogLevel();
        System.setProperty("hadoop.log.file","NN1.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new HdfsConfiguration();
        setHdfsCommonConf(conf);
        DefaultMetricsSystem.setMiniClusterMode(true);
        NameNode nn1 =  NameNode.createNameNode(new String[] {}, setHdfsHAConf(conf,1,true));
        nn1.getRpcServer().transitionToActive(
            new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource.REQUEST_BY_USER_FORCED));
        System.in.read();
    }

    @Test
    public void testNN2() throws Exception {
        clearRMClassPath();
        setLogLevel();
        System.setProperty("hadoop.log.file","NN2.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new HdfsConfiguration();
        setHdfsCommonConf(conf);
        DefaultMetricsSystem.setMiniClusterMode(true);
        ExitUtil.disableSystemExit();
        //同步主NN元数据结构
        try {
            NameNode.createNameNode(new String[] {"-bootstrapStandby"}, setHdfsHAConf(conf,2,true));
        } catch (Exception e){
            System.out.println("****************************  bootstrapStandby");
        }
        NameNode nn2 = NameNode.createNameNode(new String[] {}, conf);
        conf.set("fs.defaultFS","hdfs://minidfs-ns");
        conf.unset("ipc.client.fallback-to-simple-auth-allowed");
        nn2.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/core-site.xml")));
        nn2.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
        //conf.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/yarn-site.xml")));
        nn2.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/hdfs-site.xml")));
        nn2.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
        nn2.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/hdfs-site.xml")));
        nn2.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
        System.out.println("*********************** 文件写入成功!");
        System.in.read();
    }



    @Test
    public void DN1() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","DN1.log");
        DataNode dn = DataNode.instantiateDataNode(null, getDNConf(1));
        dn.runDatanodeDaemon();
        System.in.read();
    }

    @Test
    public void DN2() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","DN2.log");
        DataNode dn = DataNode.instantiateDataNode(null, getDNConf(2));
        dn.runDatanodeDaemon();
        System.in.read();
    }

    @Test
    public void DN3() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","DN3.log");
        DataNode dn = DataNode.instantiateDataNode(null, getDNConf(3));
        dn.runDatanodeDaemon();
        System.in.read();
    }

    @Test
    public void testZK() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","ZK.log");
        TestingServer zkServer = new TestingServer(new InstanceSpec(new File("/data/zkData"), 2181, -1, -1, false, -1),true);
        zkServer.start();
        System.in.read();
    }

    @Test
    public void testRM() throws Exception {
        clearRMClassPath();
        setLogLevel();
        System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%m%n");
        System.setProperty("hadoop.log.file","RM.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new YarnConfiguration();

        //Loaded properties from hadoop-metrics2-resourcemanager.properties
        System.setProperty("hadoop.log.file","ResourceManager_metrics.log");
        System.setProperty("java.security.krb5.conf",projectPath+"/target/test-classes/krb5.conf");
        //不设置会提示MRAppMaster 类找不到
        conf.setBoolean("yarn.minicluster.use-rpc", true);
        conf.setBoolean("yarn.is.minicluster", true);

        conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
        conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);

        conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        conf.set("yarn.nodemanager.aux-services.mapreduce_shuffle.class","org.apache.hadoop.mapred.ShuffleHandler");

        conf.set("mapreduce.framework.name","yarn");

        conf.set("yarn.node-labels.enabled","true");
        conf.set("yarn.node-labels.fs-store.root-dir","file:///lable");
        conf.set("yarn.node-attribute.fs-store.root-dir","/node-attribute");

        conf.set("yarn.resourcemanager.recovery.enabled","false");
        conf.set("yarn.resourcemanager.store.class","org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore");
        conf.set("yarn.resourcemanager.fs.state-store.uri","/rmstore");

        conf.set("yarn.scheduler.capacity.resource-calculator","org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
        //conf.set("yarn.resourcemanager.scheduler.class","org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");
        conf.set("yarn.resourcemanager.scheduler.class","org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
        conf.set("yarn.scheduler.capacity.label-metrics.enable","true");
        conf.set("yarn.resourcemanager.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("yarn.resourcemanager.keytab","/Users/temp/zhangxiping.keytab");

        //conf.set("yarn.resourcemanager.scheduler.class","org.apache.hadoop.yarn.sls.scheduler.SLSCapacityScheduler");
        //conf.set("yarn.resourcemanager.webapp.address","127.0.0.1:8088");
        conf.set("yarn.log-aggregation-enable","true");
        conf.set("yarn.log-aggregation.file-formats","TFile");
        conf.set("yarn.log-aggregation.file-controller.TFile.class","org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController");
        conf.set("mapreduce.jobhistory.address","0.0.0.0:10021");
        conf.set("mapreduce.jobhistory.webapp.address","0.0.0.0:19888");
        conf.set("yarn.nodemanager.log-aggregation.queue-monitoring-interval-seconds","10");
        conf.unset("dfs.http.policy");
        conf.set("yarn.resourcemanager.zk-address", "127.0.0.1:2181");

        ResourceManager rm1 = new ResourceManager();
        rm1.init(conf);
        rm1.start();

        Configuration config = rm1.getConfig();

        //不能直接调用conf.writeXml，使用yrCluster.getConfig() ,防止writeXml 失败
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/yarn-site.xml")));
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/mapred-site.xml")));
        //testjob 最后加载的资源文件是hdfs-site文件,这个配置是mared-default配置的local,会覆盖yarn-site的配置,需要覆盖
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
        //客户端设置RM主备,方便命令行操作 , 要使用hadoop3.3.0 客户端执行yarn rmadmin ,单测试用例没有这个问题
        //config.set("yarn.resourcemanager.admin.address.rm1", "127.0.0.1:18033");
        //config.set("yarn.resourcemanager.admin.address.rm2","127.0.0.1:28033");
        config.writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/yarn-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/yarn-site.xml")));

        System.in.read();
    }

    @Test
    public void testRM1() throws Exception {
        clearRMClassPath();
        setLogLevel();
        System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%m%n");
        System.setProperty("hadoop.log.file","RM1.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new YarnConfiguration();
        ResourceManager rm1 = new ResourceManager();
        rm1.init(getRMConf(conf,1));
        rm1.start();
        System.in.read();
    }

    @Test
    public void testRM2() throws Exception {
        setLogLevel();
        System.setProperty("log4j.appender.RFAMETRIC.layout.ConversionPattern","%m%n");
        System.setProperty("hadoop.log.file","RM2.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new YarnConfiguration();
        ResourceManager rm1 = new ResourceManager();
        rm1.init(getRMConf(conf,2));
        rm1.start();
        Thread.sleep(3000);
        Configuration config = rm1.getConfig();

        //不能直接调用conf.writeXml，使用yrCluster.getConfig() ,防止writeXml 失败
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/yarn-site.xml")));
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/mapred-site.xml")));
        //testjob 最后加载的资源文件是hdfs-site文件,这个配置是mared-default配置的local,会覆盖yarn-site的配置,需要覆盖
        config.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
        //客户端设置RM主备,方便命令行操作 , 要使用hadoop3.3.0 客户端执行yarn rmadmin ,单测试用例没有这个问题
        //config.set("yarn.resourcemanager.admin.address.rm1", "127.0.0.1:18033");
        //config.set("yarn.resourcemanager.admin.address.rm2","127.0.0.1:28033");
        config.writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/yarn-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
        config.writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/yarn-site.xml")));
        System.out.println("*********************** 文件写入成功!");
        System.in.read();
    }

    /**
     *
     */
    @Test
    public void testHS() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","JHS.log");
        Configuration conf =new Configuration();
        System.setProperty("hadoop.root.logger","DEBUG,stdout");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");

        //conf.set("dfs.http.policy","HTTPS_ONLY");//其他服务配置可能有问题
        //conf.set("yarn.https.policy","HTTP");

        conf.set("mapreduce.jobhistory.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("mapreduce.jobhistory.keytab","/Users/temp/zhangxiping.keytab");
        //conf.set("mapreduce.jobhistory.http.policy","HTTPS_ONLY");
        conf.unset("dfs.http.policy");
        System.out.println("**************** dfs.http.policy"+ conf.get("dfs.http.policy"));
        conf.set("mapreduce.jobhistory.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
        conf.set("mapreduce.jobhistory.keytab","/Users/temp/zhangxiping.keytab");
        conf.set("yarn.log-aggregation-enable","true");

        //conf.set("mapreduce.jobhistory.intermediate-done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate");
        //conf.set("mapreduce.jobhistory.done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done");

        JobHistoryServer HS = new JobHistoryServer();
        HS.init(conf);
        HS.start();
        System.in.read();
    }

    @Test
    public void testNM1() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","NM.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        NodeManager nm = new NodeManager();
        Configuration conf = getNMConf(13562,1);
        nm.init(conf);
        nm.start();
        System.in.read();
    }

    @Test
    public void testNM2() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","NM2.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        NodeManager nm = new NodeManager();
        Configuration conf = getNMConf(13563,2);
        nm.init(conf);
        nm.start();
        System.in.read();
    }

    @Test
    public void testNM3() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","NM3.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        NodeManager nm = new NodeManager();
        Configuration conf = getNMConf(13564,3);
        nm.init(conf);
        nm.start();
        System.in.read();
    }

    @Test
    public void testNM4() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","NM4_metrics.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        NodeManager nm = new NodeManager();
        Configuration conf = getNMConf(13566,4);
        nm.init(conf);
        nm.start();
        System.in.read();
    }

    @Test
    public void testNM5() throws Exception {
        setLogLevel();
        System.setProperty("hadoop.log.file","NM5_metrics.log");
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
        NodeManager nm = new NodeManager();
        Configuration conf = getNMConf(13567,5);
        nm.init(conf);
        nm.start();
        System.in.read();
    }
}
