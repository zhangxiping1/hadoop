package org.apache.hadoop.mapreduce.v2;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

/**
 * 还需要修改  MiniDFSCluster   1455  conf.set("dfs.http.policy","HTTPS_ONLY");
 *
 *  1506  // Set up datanode address
 *       conf.set("dfs.datanode.address","0.0.0.0:"+6100+i);
 *       conf.set("dfs.datanode.http.address","0.0.0.0:"+6101+i);
 *       conf.set("dfs.datanode.https.address","0.0.0.0:"+6102+i);
 *       setupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
 *
 *
 * 拷贝  KDC 类 至  D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\src\test\java\org\apache\hadoop\mapreduce\v2\MiniKdc.java
 *
 * "ssl.server.keystore.location","/Users/tempproject\\neproject\\ne-hadoop\\keystore.jks"
 *
 * https://blog.csdn.net/anxia5150/article/details/101966285
 * datanode  启动  报ssl config文件找不到  server.location     ,test-class  目录拷贝  桌面ssl-server文件，ssl-client文件
 *
 *
 *  kinit -kt C:\Users\ZHANGX~1\AppData\Local\Temp\zhangxiping.keytab zhangxiping/127.0.0.1@EXAMPLE.COM
 *
 *  拷贝 D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-minikdc\src\main\resources\ *.diff 文件   放入  test-class
 *
 *  webapps/hdfs  not found   , 在D:\project\neproject\3\ne-hadoop\  执行  mvn clean package -Dmaven.javadoc.skip=true -Pdist -DskipTests
 *
 *
 *  TestAclsEndToEnd  报 符号找不到   运行一下这里面的测试用例
 *
 *  import org.apache.hadoop.minikdc.KerberosSecurityTestcase;    minikdc 找不到  运行一下  TestMiniKdc.testKeytabGen
 *
 *  编译   使用  jdk  1.8
 *
 *  idea  generated-sources     不生效   右侧 maven reload
 *
 * java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$POSIX.stat(Ljava/lang/String;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
 * Edit configuration  -》  PATH=D:\project\neproject\3.3.0\ne-hadoop\hadoop-common-project\hadoop-common\target\bin;%PATH%
 *
 *
 * for  mac
 *
 * Native_lib    或     Secure IO is not possible without native code extensions
 *
 * 拷贝  hadoop-dist  target/lib/native    /Users/zhangxiping/Library/Java/Extensions
 *
 */

public class TestClusterNoHA {
  private MiniKMS miniKMS;
  private MiniKdc kdc;
  private File workDir;
  static String projectPath = new File("").getAbsolutePath();
  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();
  static String NAMESERVICE = "minidfs-ns";
  static String NN1 = "nn1";
  static String NN2 = "nn2";
  static void createFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("test test  test  test  something");
    a_out.close();
  }

  private static String prepareDirs(String dirType, int numDirs,int index) {
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
  static Configuration getNMconf(int port,int index){
    System.setProperty("hadoop.log.dir",projectPath +"/target/log");

    //conf.addResource(new Path(new File("/Users/temp/yarn-site.xml").getAbsolutePath()));
    // verify state store is not removed on normal shutdown
    Configuration config = new YarnConfiguration();
    // create nm-local-dirs and configure them for the nodemanager
    String localDirsString = prepareDirs("local", 1,index);
    config.set(YarnConfiguration.NM_LOCAL_DIRS, localDirsString);
    // create nm-log-dirs and configure them for the nodemanager
    String logDirsString = prepareDirs("log", 1,index);
    config.set(YarnConfiguration.NM_LOG_DIRS, logDirsString);

    config.setInt(YarnConfiguration.NM_PMEM_MB, config.getInt(
        YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB,
        YarnConfiguration.DEFAULT_YARN_MINICLUSTER_NM_PMEM_MB));

    config.set(YarnConfiguration.NM_ADDRESS,
        MiniYARNCluster.getHostname() + ":0");
    config.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
        MiniYARNCluster.getHostname() + ":0");
    config.set(YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
        MiniYARNCluster.getHostname() + ":0");
    WebAppUtils
        .setNMWebAppHostNameAndPort(config,
            MiniYARNCluster.getHostname(), 0);

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

    //查看task日志时， 先访问NM web端口， NM请求historyserver获取日志
    //http://hih-d-24453.EXAMPLE.COM:1547/node/containerlogs/container_e01_1640673394737_0001_01_000002/zhangxiping/stderr?start=-4096  NM 访问 historyserver
    //NM webapp启动端口：NM  Web app node started at 1547
    config.set("yarn.log.server.url", "http://0.0.0.0:19888/jobhistory/logs");
    config.unset("dfs.http.policy");
    config.set("yarn.node-labels.enabled","true");
    config.set("yarn.nodemanager.principal","zhangxiping/127.0.0.1@EXAMPLE.COM");
    config.set("yarn.nodemanager.keytab","/Users/temp/zhangxiping.keytab");

    return config;
  }

  static Configuration getDNConf(int i) throws IOException {

    System.setProperty("hadoop.log.dir",projectPath +"/target/log");
    System.setProperty("hadoop.log.file","DN"+i+"-metrics.log");
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");
    conf.setLong(DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 0);
    conf.set("dfs.http.policy","HTTPS_ONLY");
    conf.set("dfs.datanode.address","0.0.0.0:"+6100+i);
    conf.set(" dfs.datanode.ipc.address","0.0.0.0:"+987+i);
    conf.set("dfs.datanode.http.address","0.0.0.0:"+6101+i);
    conf.set("dfs.datanode.https.address","0.0.0.0:"+6102+i);
    conf.set("dfs.blockreport.intervalMsec","30000");
    String base_dir = projectPath+"/target/test-dir/dfs";
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

  static void clearRMClassPath(){
    File mapRed_site = new File(projectPath + "/target/test-classes/mapred-site.xml");
    mapRed_site.delete();
    File yarn_site = new File(projectPath + "/target/test-classes/yarn-site.xml");
    yarn_site.delete();
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
    System.setProperty("hadoop.log.dir",projectPath +"/target/log");
  }

  static Configuration setHdfsConf(Configuration conf ,boolean format) throws IOException {

    conf.set("fs.defaultFS","hdfs://127.0.0.1:8020");
    conf.set("dfs.namenode.rpc-address","127.0.0.1:8020");
    conf.set("dfs.namenode.name.dir",projectPath + "/target/test-dir/dfs/name");
    conf.set("dfs.namenode.checkpoint.dir",projectPath + "/target/test-dir/dfs/namesecondary");

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

    if (format) {
      HdfsServerConstants.StartupOption.FORMAT.setClusterId("minidfs-ns");
      DFSTestUtil.formatNameNode(conf);
    }
    return conf;
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
    conf.set("dfs.web.authentication.kerberos.principal","HTTP/127.0.0.1@EXAMPLE.COM");
    conf.set("dfs.web.authentication.kerberos.keytab","/Users/temp/zhangxiping.keytab");
    conf.set("dfs.namenode.kerberos.internal.spnego.principal","HTTP/127.0.0.1@EXAMPLE.COM");
    conf.set("hadoop.http.authentication.type","simple");
    conf.set("hadoop.http.authentication.kerberos.principal","HTTP/127.0.0.1@EXAMPLE.COM");
    conf.set("hadoop.http.authentication.kerberos.keytab","/Users/temp/zhangxiping.keytab");

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
  }

  @Test
  public void testKDC() throws Exception {
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
    conf.set("hadoop.security.auth_to_local","RULE:[2:$1@$0](.*@EXAMPLE.COM)s/.*/zhangxiping/\n"+"DEFAULT");
    // NN 可能依赖环境变量里面的配置

    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    String [] principals = new String[]{"zhangxiping/127.0.0.1","HTTP/127.0.0.1"};
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

    final String keystore;
    final String password;
    File kmsDir = new File(projectPath +"/target/test-classes/" );
    Configuration conf =new Configuration();
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

  @Test
  public void testNN() throws Exception {
    clearClassPath();
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    Configuration conf = new HdfsConfiguration();
    setHdfsCommonConf(conf);
    DefaultMetricsSystem.setMiniClusterMode(true);
    NameNode nn =  NameNode.createNameNode(new String[] {}, setHdfsConf(conf,false));

    conf.set("fs.defaultFS","hdfs://127.0.0.1:8020");
    nn.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/core-site.xml")));
    nn.getConf().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));
    //onf.writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/yarn-site.xml")));
    nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/hdfs-site.xml")));
    nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
    nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/hdfs-site.xml")));
    nn.getConf().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
    System.in.read();
  }

  @Test
  public void DN1() throws Exception {
    DataNode dn = DataNode.instantiateDataNode(null, getDNConf(1));
    dn.runDatanodeDaemon();
    System.in.read();
  }

  @Test
  public void DN2() throws Exception {
    DataNode dn = DataNode.instantiateDataNode(null, getDNConf(2));
    dn.runDatanodeDaemon();
    System.in.read();
  }

  @Test
  public void DN3() throws Exception {
    DataNode dn = DataNode.instantiateDataNode(null, getDNConf(3));
    dn.runDatanodeDaemon();
    System.in.read();
  }

  @Test
  public void testRM() throws Exception {
    System.setProperty("hadoop.log.file","ResourceManager_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf","/target/test-classes/krb5.conf");
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC,true);
    conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
    conf.set("yarn.nodemanager.aux-services.mapreduce_shuffle.class","org.apache.hadoop.mapred.ShuffleHandler");
    conf.set("mapreduce.framework.name","yarn");
    conf.set("yarn.resourcemanager.ha.enabled","false");
    conf.set("yarn.node-labels.enabled","true");
    conf.set("yarn.node-labels.fs-store.root-dir","/lable");

    conf.set("yarn.resourcemanager.recovery.enabled","true");
    conf.set("yarn.resourcemanager.store.class","org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore");
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
    //conf.unset("hadoop.caller.context.enabled");
    conf.unset("dfs.datanode.data.dir");
    //router
    //conf.set("fs.defaultFS","hdfs://127.0.0.1:40250");

    MiniYARNCluster yrCluster =new MiniYARNCluster("test",1,0,1,1);
    yrCluster.init(conf);
    yrCluster.start();
    yrCluster.getConfig().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/yarn-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/mapred-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File(projectPath + "/target/test-classes/hdfs-site.xml")));

    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/core-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/yarn-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-2.9.2-1.1.1.5/etc/hadoop/hdfs-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/core-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/yarn-site.xml")));
    yrCluster.getConfig().writeXml(new FileOutputStream(new File("/hadoop-3.3.0-1.1.1/etc/hadoop/hdfs-site.xml")));
    System.in.read();
  }

  /**
   * history 查不到连接是  mapreduce.jobhistory.intermediate-done-dir， mapreduce.jobhistory.done-dir 问题
   *
   * 点击yarn webui 不能正常跳转  conf.set("mapreduce.jobhistory.webapp.address","0.0.0.0:19888");
   *
   */
  @Test
  public void testHS() throws Exception {
    System.setProperty("hadoop.log.file","historyServer_metrics.log");
    Configuration conf =new Configuration();
    System.setProperty("HADOOP_USER_NAME","root");
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

    conf.set("mapreduce.jobhistory.intermediate-done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate");
    conf.set("mapreduce.jobhistory.done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done");

    JobHistoryServer historyServer = new JobHistoryServer();
    historyServer.init(conf);
    historyServer.start();
    System.in.read();
  }

  @Test
  public void testNM() throws Exception {
    System.setProperty("hadoop.log.file","NM_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    //System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
    NodeManager nm = new NodeManager();
    Configuration conf = getNMconf(13562,1);
    conf.set("yarn.nodemanager.address","0.0.0.0:25941");
    nm.init(conf);
    nm.start();
    System.in.read();
  }

  @Test
  public void testNM2() throws Exception {
    System.setProperty("hadoop.log.file","NM2_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    //System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
    NodeManager nm = new NodeManager();
    Configuration conf = getNMconf(13563,2);
    conf.set("yarn.nodemanager.address","0.0.0.0:25942");
    nm.init(conf);
    nm.start();
    System.in.read();
  }

  @Test
  public void testNM3() throws Exception {
    System.setProperty("hadoop.log.file","NM3_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    //System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
    NodeManager nm = new NodeManager();
    Configuration conf = getNMconf(13564,3);
    conf.set("yarn.nodemanager.address","0.0.0.0:25943");
    nm.init(conf);
    nm.start();
    System.in.read();
  }

  @Test
  public void testNM4() throws Exception {
    System.setProperty("hadoop.log.file","NM4_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
    NodeManager nm = new NodeManager();
    Configuration conf = getNMconf(13566,4);
    conf.set("yarn.nodemanager.address","0.0.0.0:25944");
    nm.init(conf);
    nm.start();
    System.in.read();
  }

  @Test
  public void testNM5() throws Exception {
    System.setProperty("hadoop.log.file","NM5_metrics.log");
    UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
    System.setProperty("HADOOP_USER_NAME","root");
    System.setProperty("java.security.krb5.conf",projectPath + "/target/test-classes/krb5.conf");
    NodeManager nm = new NodeManager();
    Configuration conf = getNMconf(13567,5);
    conf.set("yarn.nodemanager.address","0.0.0.0:25945");
    nm.init(conf);
    nm.start();
    System.in.read();
  }

  @Test
  public void testZK() throws Exception {
    TestingServer zkServer = new TestingServer(new InstanceSpec(new File("/Users/temp/zkData"), 2181, -1, -1, false, -1),true);
    zkServer.start();



    System.in.read();
  }

}

