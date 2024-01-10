package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.StringTokenizer;
import java.util.UUID;

import static org.apache.hadoop.ipc.TestRpcBase.newEmptyRequest;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testjob {

    static String projectPath = new File("").getAbsolutePath();
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                Thread.sleep(30000);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    /**
     *  加载的环境变量 ，只有 hadoop-common\target\classes 有 core-default.xml ,其他模块里只有test resource有core-site.xml
     *  看下面的加载顺序，只会加载 hadoop-common\target\classes 的core-default.xml ，以及我们写入的 hadoop-mapreduce-client-jobclient\target\test-classes
     *     D:\project\neproject\ne-hadoop\hadoop-tools\hadoop-distcp\target\test-classes
     *     D:\project\neproject\ne-hadoop\hadoop-tools\hadoop-distcp\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-common\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-auth\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-annotations\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-app\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-common\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-server\hadoop-yarn-server-web-proxy\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-server\hadoop-yarn-server-common\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-registry\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-api\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-shuffle\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-server\hadoop-yarn-server-nodemanager\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-hs\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-core\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-client\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-yarn-project\hadoop-yarn\hadoop-yarn-common\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-kms\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\target\test-classes
     *     D:\project\neproject\ne-hadoop\hadoop-hdfs-project\hadoop-hdfs-client\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-hdfs-project\hadoop-hdfs\target\classes
     *     D:\project\neproject\ne-hadoop\hadoop-hdfs-project\hadoop-hdfs\target\test-classes
     *     D:\project\neproject\ne-hadoop\hadoop-common-project\hadoop-common\target\test-classes
     */

    //访问 非安全集群 ，删除
    // D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\target\classes\
    // core-site.xml  hdfs-site.xml  mapred-site.xml  yarn-site.xml

    static void clearClassPath(){
        String Path = projectPath+"/../../hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient";
        File core_site = new File(Path + "/target/test-classes/core-site.xml");
        core_site.delete();
        File hdfs_site = new File(Path + "/target/test-classes/hdfs-site.xml");
        hdfs_site.delete();
        File mapRed_site = new File(Path + "/target/test-classes/mapred-site.xml");
        mapRed_site.delete();
        File yarn_site = new File(Path + "/target/test-classes/yarn-site.xml");
        yarn_site.delete();
    }

    @Test
    public void testHdfsApiAddFileNOKerBeros() throws Exception {
        clearClassPath();
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:40250");
        conf.set("ipc.client.rpc-timeout.ms","120000");
        conf.set("ipc.ping.interval","120000");
        conf.get("ipc.ping.interval");
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        System.out.println(fs.rename(new Path("/user/d/a/1.txt"),new Path("/user/d/a")));


        //System.out.println("*******************"+ conf.get("zhangxiping"));
//        fs.delete(new Path("/a"),true);
//        fs.mkdirs(new Path("/a"));
//        for(int i=0;i<5;i++){
//            createFile(fs,new Path("/a/"+i));
//            System.out.println("Created /a/"+i);
//        }
    }

    @Test
    public void testHdfsApiAddFile() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new HdfsConfiguration();
        conf.set("ipc.client.rpc-timeout.ms","120000");
        conf.set("ipc.ping.interval","120000");
        conf.get("ipc.ping.interval");
        //krb5.conf 文件在环境变量里面没有用 ,必须是系统变量指定位置, pom 继承设置了
        // window 默认会加载  C://windows/krb5.conf
        //System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        System.out.println("java.security.krb5.conf = " + System.getProperty("java.security.krb5.conf"));
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        //System.out.println("*******************"+ conf.get("zhangxiping"));
//        fs.mkdirs(new Path("/user/d/staing"));
//        createFile(fs,new Path("/user/d/staing/a.txt"));
//        fs.rename(new Path("/user/d/staing/a.txt"),new Path("/user/d"));
//        createFile(fs,new Path("/user/d/staing/a.txt"));

//        fs.rename(new Path("/user/d/staing/a.txt"),new Path("/user/d"), Options.Rename.NONE);
        fs.delete(new Path("/a"),true);
        fs.mkdirs(new Path("/a"));
        for(int i=0;i<100;i++){
            createFile(fs,new Path("/a/"+i));
            System.out.println("Created /a/"+i);
        }
    }

    //  任务默认加载的环境变量路径  D:\project\neproject\ne-hadoop\hadoop-mapreduce-project\hadoop-mapreduce-client\hadoop-mapreduce-client-jobclient\target\classes
    //  访问router  把这里面的  fs.defaultFS 改了

    // appmaster  container 报 hadoop_home 未设置 ，NM 在生成launch_container脚本，会通过白名单，添加脚本环境变量，默认的白名单没有hadoop home


    // yarn HA webUI 重定向次数过多,手动切换rm2 主

    //加载core-site  路径
    //******** file:/D:/project/neproject/3.3.0/ne-hadoop/hadoop-common-project/hadoop-common/target/classes/core-default.xml
    // 删掉  ******** file:/D:/project/neproject/3.3.0/ne-hadoop/hadoop-tools/hadoop-distcp/target/classes/core-site.xml

    @Test
    public void mapreduceJob() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();
        //System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("hadoop.root.logger","INFO,stdout");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        conf.set("mapreduce.task.timeout","60000");
        conf.set("dfs.replication","3");
        conf.set("mapred.child.java.opts","-Dfile.encoding=UTF-8");
        // 想查看中间生成的临时文件 ，通过设置AM启动debug调试
        conf.set("yarn.app.mapreduce.am.command-opts","-Dfile.encoding=UTF-8 ");//-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=9906
        conf.set("mapreduce.job.queuename","root.pro");
        System.setProperty("file.encoding","utf-8");
        Long startTs = System.currentTimeMillis();

        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        fs.delete(new Path("/a"),true);
        fs.mkdirs(new Path("/a"));
        for(int i=0;i<10;i++){
            createFile(fs,new Path("/a/"+i));
            System.out.println("Created /a/"+i);
        }

        //conf.addResource(new Path("/Users/temp/yarn-site.xml"));
        String[] args = new String[]{"/a/3","/user/zhangxiping/"+startTs};
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "wordCount");
        job.setJarByClass(Testjob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    @Test
    public void mapreduceJobNOKerBose() throws Exception {
        clearClassPath();
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:40250");
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("hadoop.root.logger","INFO,stdout");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        conf.set("mapreduce.task.timeout","60000");
        conf.set("mapreduce.job.queuename","root.da_music.sla");
        conf.set("file.encoding","utf-8");
        Long startTs = System.currentTimeMillis();
        conf.set("dfs.replication","3");
        String[] args = new String[]{"/a","/user/zhangxiping/"+startTs};
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "wordCount");
        job.setJarByClass(Testjob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    @Test
    public void testJoB() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf =new Configuration();
        System.setProperty("HADOOP_USER_NAME","zhangxiping");
        System.setProperty("hadoop.root.logger","INFO,stdout");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        conf.set("mapreduce.task.timeout","60000");
        conf.set("file.encoding","utf-8");
        conf.set("mapreduce.job.queuename","root.pro");
        conf.set("mapreduce.jobhistory.address","127.0.0.1:19888");
        conf.set("mapreduce.framework.name","yarn");
        String[] args = new String[] {
            "-skipcrccheck",
            "-update",
            "-m",
            String.valueOf(10),
            "hdfs://127.0.0.1:8020/a/2",
            "hdfs://127.0.0.1:8020/user/zhangxiping"

        };
        ToolRunner.run(conf, new DistCp(), args);
    }

    @Test
    public void testJoB2() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf =new Configuration();
        System.setProperty("HADOOP_USER_NAME","zhangxiping");
        System.setProperty("hadoop.root.logger","INFO,stdout");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        conf.set("mapreduce.task.timeout","60000");
        conf.set("file.encoding","utf-8");
        conf.set("mapreduce.job.queuename","root.dev.more");
        conf.set("mapreduce.jobhistory.address","127.0.0.1:19888");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
        conf.set("dfs.permissions","false");
        conf.addResource(new Path("D:\\project\\neproject\\ne-hadoop\\hadoop-tools\\hadoop-sls\\src\\main\\sample-conf\\yarn-site.xml"));
        String[] args = new String[] {
            "-m",
            String.valueOf(10),
            "hdfs://127.0.0.1:8020/a",
            "hdfs://127.0.0.1:8020/user/zhangxiping"
        };
        ToolRunner.run(conf, new DistCp(), args);
    }



    @Test
    public void testHdfsJob() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();
        conf.addResource(new Path(projectPath+ "/Users/temp/yarn-site.xml"));
        conf.set("hadoop.security.authorization","true");
        conf.set("hadoop.security.authentication","kerberos");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);

        ContentSummary a = fs.getContentSummary(new Path("/c"));


        FileStatus[] fsStatus = fs.listStatus(new Path("/c"));
        for (int i = 0;i < fsStatus.length;i++){
            System.out.println(fsStatus[i].getPath().toString());
        }

        FileStatus[] fsStatus2 = fs.listStatus(new Path("/"));
        for (int i = 0;i < fsStatus2.length;i++){
            System.out.println(fsStatus[i].getPath().toString());
        }
    }

    @Test
    public void testHdfsApi() throws IOException {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();

        conf.set("ipc.client.rpc-timeout.ms","120000");
        conf.set("ipc.ping.interval","120000");
        conf.set("ipc.client.fallback-to-simple-auth-allowed","true");
        conf.set("dfs.client.socket-timeout","3000000");
        conf.set("dfs.replication","2");
        System.setProperty("java.security.krb5.conf",projectPath+ "/target/test-classes/krb5.conf");
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        //
        long time = System.currentTimeMillis();
        createFile(fs,new Path("/key1/"+time+".txt"));
    }

    //main 线程
    // ->  FSDataOutputStream.create()
    // -> DFSClient.create()
    // ->  DFSOutputStream.getStreamer().start
    //Streamer线程. dataQueue
    //main 线程写trunk 放入 dataQueue ，Streamer线程读trunk 发送
    static void createFile(FileSystem fs, Path f) throws IOException {
        DataOutputStream a_out = fs.create(f);
        for (int i = 0;i < 10000;i++){
            a_out.writeBytes("a");
        }
        a_out.close();
    }

    static void getFile(FileSystem fs, Path f) throws IOException {
        FSDataInputStream in = fs.open(f);
        byte buf[] = new byte[1024];
        int bytesRead = in.read(buf);
        String path = projectPath+ "/target/tmp/"+System.currentTimeMillis()+".txt";
        FileOutputStream out = new FileOutputStream(path);
        System.out.println("************path :"+path);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            bytesRead = in.read(buf);
        }
        out.close();
        in.close();

    }

    static void createFile(FileSystem fs, String src,Path f) throws IOException {
        DataOutputStream a_out = fs.create(f);
        FileInputStream in = new FileInputStream(src);
        byte buf[] = new byte[1024];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            a_out.write(buf, 0, bytesRead);
            bytesRead = in.read(buf);
        }
        a_out.close();
        in.close();
    }

    @Test
    public void testHdfsConcurrentApi() throws IOException {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM","/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();

        conf.set("ipc.client.rpc-timeout.ms", "120000");
        conf.set("ipc.ping.interval", "120000");
        conf.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        conf.set("dfs.client.socket-timeout", "3000000");
        conf.set("dfs.replication", "3");
        System.setProperty("java.security.krb5.conf", projectPath + "/target/test-classes/krb5.conf");
        conf.set("fs.hdfs.impl.disable.cache", "true");
        //
        long time = System.currentTimeMillis();
        for (int i = 0;i < 10;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0;j < 500;j++){
                            DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
                            Path a =new Path("/a/"+ UUID.randomUUID().toString());
                            System.out.println(Thread.currentThread().getName()+":createFile:"+a.toString());
                            createFile(fs,a);
                            System.out.println(
                                Thread.currentThread().getName() + ":createFileEnd:listStatus:/b");
                            fs.listStatus(new Path("/a"));
                            System.out.println(
                                Thread.currentThread().getName() + ":listStatus:/a end ,rename");
                            fs.rename(a, new Path(a.toString() + "_rename"));
                            System.out.println(
                                Thread.currentThread().getName() + ":rename end ,delete");
                            fs.delete(new Path(a.toString() + "_rename"), true);
                            System.out.println(Thread.currentThread().getName()
                                + ":delete end ,listEncryptionZones");
                            fs.listEncryptionZones();
                            System.out.println(
                                Thread.currentThread().getName() + ":listEncryptionZones end");
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        System.in.read();
    }

    @Test
    public void testYarnApi() throws Exception {
        YarnClientImpl client = (YarnClientImpl) YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        try {
            client.init(conf);
            client.start();
            client.getClusterNodeLabels();
            client.getLabelsToNodes();
            client.getLabelsToNodes();
        } finally {
            client.stop();
        }
    }

    //Token 方式连接NN测试
    @Test
    public void testToken() throws Exception {
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM", "/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:40250");
        // Get DelegationToken for the user
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        Token<DelegationTokenIdentifier> token = ugi.doAs(new PrivilegedExceptionAction<Token>() {
            @Override
            public Token<DelegationTokenIdentifier> run() throws Exception {
                return (Token<DelegationTokenIdentifier>) FileSystem.get(conf).getDelegationToken("yarn");
            }
        });
        // 备节点的地址
        InetSocketAddress server = new InetSocketAddress("127.0.0.1",40250);
        SecurityUtil.setTokenService(token, server);
        ugi.addToken(token);
        RPC.setProtocolEngine(conf,ClientProtocol.class, WritableRpcEngine.class);
        ClientProtocol proxy = RPC.getProxy(ClientProtocol.class, 0, server, conf);
        //QOP must be auth
        for(int i=0;i<100;i++){
            try{
                proxy.getFileInfo("/a");
            }catch ( Exception e){
            }
        }
    }

    @Test
    public void testdeleteToken() throws Exception {
        UserGroupInformation.loginUserFromKeytab("HTTP/127.0.0.1@EXAMPLE.COM", "/Users/temp/zhangxiping.keytab");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:40250");
        // Get DelegationToken for the user
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        Token<DelegationTokenIdentifier> token = ugi.doAs(new PrivilegedExceptionAction<Token>() {
            @Override
            public Token<DelegationTokenIdentifier> run() throws Exception {
                return (Token<DelegationTokenIdentifier>) fs.getDelegationToken("zhangxiping");
            }
        });
        RPC.setProtocolEngine(conf,ClientProtocol.class, WritableRpcEngine.class);

        //
        System.out.println("**********"+fs.getFileStatus(new Path("/a")));
        //Thread.sleep(30000);

        // 备节点的地址
        InetSocketAddress server = new InetSocketAddress("127.0.0.1",40251);
        SecurityUtil.setTokenService(token, server);
        ugi.addToken(token);
//        ClientProtocol proxy = RPC.getProxy(ClientProtocol.class, 69L, server, conf);
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:40250");
        DistributedFileSystem fs2 = (DistributedFileSystem)FileSystem.get(conf);
        fs2.getFileStatus(new Path("/a"));
       // Thread.sleep(30000);
       // fs2.getClient().cancelDelegationToken(token);
        InetSocketAddress server2 = new InetSocketAddress("127.0.0.1",40250);
        SecurityUtil.setTokenService(token, server2);
        ugi.logoutUserFromKeytab();
        UserGroupInformation.loginUserFromKeytab("zhangxiping/127.0.0.1@EXAMPLE.COM", "/Users/temp/zhangxiping.keytab");
        fs2.getClient().renewDelegationToken(token);
    }
}
