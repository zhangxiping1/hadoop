package org.apache.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.util.LogFormatter;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class TestCluster {

  private static final Logger LOG =
      LogFormatter.getLogger("org.apache.hadoop.dfs.TestDFS");

  private static Configuration conf = new Configuration();
  private static int BUFFER_SIZE =
      conf.getInt("io.file.buffer.size", 4096);

  private static int testCycleNumber = 0;

  /**
   * all DFS test files go under this base directory
   */
  private static String baseDirSpecified;

  /**
   * base dir as File
   */
  private static File baseDir;

  /** DFS block sizes to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] BLOCK_SIZES = {100000, 4096};

  /** DFS file sizes to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] FILE_SIZES =
      {100000, 100001, 4095, 4096, 4097, 1000000, 1000001};

  /** DFS file counts to permute over in multiple test cycles
   * (array length should be prime).
   */
  private static final int[] FILE_COUNTS = {1, 10, 100};

  /** Number of useful permutations or test cycles.
   * (The 2 factor represents the alternating 2 or 3 number of datanodes
   * started.)
   */
  private static final int TEST_PERMUTATION_MAX_CEILING =
      BLOCK_SIZES.length * FILE_SIZES.length * FILE_COUNTS.length * 2;

  /** Number of permutations of DFS test parameters to perform.
   * If this is greater than ceiling TEST_PERMUTATION_MAX_CEILING, then the
   * ceiling value is used.
   */
  private static final int TEST_PERMUTATION_MAX = 3;
  private Constructor randomDataGeneratorCtor = null;

  static {
    baseDirSpecified = System.getProperty("test.dfs.data", new File("").getAbsolutePath()+ "/tmp/dfs_test");
    baseDir = new File(baseDirSpecified);
  }

  protected void setUp() throws Exception {
    conf.setInt("test.dfs.block_size", BUFFER_SIZE);
    conf.setInt("dfs.namenode.handler.count", 3);
    conf.setLong("dfs.blockreport.intervalMsec", 600*1000L);
    conf.setLong("dfs.datanode.startupMsec", 15*1000L);
    conf.setLong("dfs.replication", 3);
    conf.setBoolean("test.dfs.same.host.targets.allowed", true);
    int nameNodePort = 8020;
    String nameNodeSocketAddr = "localhost:" + nameNodePort;
    conf.set("fs.default.name", nameNodeSocketAddr);
  }

  /**
   * Remove old files from temp area used by this test case and be sure
   * base temp directory can be created.
   */
  protected void prepareTempFileSpace() {
    if (baseDir.exists()) {
      try { // start from a blank slate
        FileUtil.fullyDelete(baseDir, conf);
      } catch (Exception ignored) {
      }
    }
    baseDir.mkdirs();
    if (!baseDir.isDirectory()) {
      throw new RuntimeException("Value of root directory property test.dfs.data for dfs test is not a directory: "
          + baseDirSpecified);
    }
  }

  @Test
  public void testNN() throws Exception {
    setUp();
    String nameFSDir = baseDirSpecified + "/namenode";
    NameNode nameNode = new NameNode(new File(nameFSDir), 8020, conf);
    System.in.read();
  }

  @Test
  public void testDN1() throws Exception {
    setUp();
    String dataDir = baseDirSpecified + "/datanode1";
    conf.set("dfs.data.dir", dataDir);
    DataNode dn = DataNode.makeInstanceForDir(dataDir, conf);
    dn.run();
    System.in.read();
  }

  @Test
  public void testDN2() throws Exception {
    setUp();
    String dataDir = baseDirSpecified + "/datanode2";
    conf.set("dfs.data.dir", dataDir);
    DataNode dn = DataNode.makeInstanceForDir(dataDir, conf);
    dn.run();
    System.in.read();
  }

  @Test
  public void testDN3() throws Exception {
    setUp();
    String dataDir = baseDirSpecified + "/datanode3";
    conf.set("dfs.data.dir", dataDir);
    DataNode dn = DataNode.makeInstanceForDir(dataDir, conf);
    dn.run();
    System.in.read();
  }

  @Test
  public void testDFSClientWrite() throws Exception {
    setUp();
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 8020), conf);
//    createFile(dfsClient,"/"+System.currentTimeMillis()+".txt");
    createFile(dfsClient,baseDirSpecified+"/hadoop-default.xml","/zxp.txt");
    DFSFileInfo[] fsStatus = dfsClient.listFiles(new UTF8("/"));
    for (int i = 0;i < fsStatus.length;i++){
      System.out.println(fsStatus[i].getPath());
    }
  }

  @Test
  public void testDFSClientWrite1() throws Exception {
    setUp();
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 8020), conf);
//    createFile(dfsClient,"/"+System.currentTimeMillis()+".txt");
    createFile(dfsClient,"D:\\MemoryAnalyzer-1.14.0.20230315-win32.win32.x86_64.zip","/"+System.currentTimeMillis()+"/aaaaa/zxp.txt");
    DFSFileInfo[] fsStatus = dfsClient.listFiles(new UTF8("/"));
    for (int i = 0;i < fsStatus.length;i++){
      System.out.println(fsStatus[i].getPath());
    }
  }

  @Test
  public void testDFSClientRead() throws Exception {
    setUp();
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 8020), conf);
//    createFile(dfsClient,"/"+System.currentTimeMillis()+".txt");
    readFile(dfsClient,"/zxp.txt");
  }

  static void createFile(DFSClient dfsClient, String f) throws IOException {
    FSOutputStream a_out = dfsClient.create(new UTF8(f),false);
    a_out.write("test test  test  test  something".getBytes());
    a_out.close();
    dfsClient.lock(new UTF8(f),false);
  }

  static void createFile(DFSClient dfsClient, String src, String f) throws IOException {
    FSOutputStream a_out = dfsClient.create(new UTF8(f),false);
    FileInputStream in = new FileInputStream(src);
    byte buf[] = new byte[1024];
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      a_out.write(buf, 0, bytesRead);
      bytesRead = in.read(buf);
    }
    a_out.close();
  }

  static void readFile(DFSClient dfsClient, String f) throws IOException {
    FSInputStream in = dfsClient.open(new UTF8(f));
    byte buf[] = new byte[1024];
    StringBuilder stringBuilder = new StringBuilder();
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buf); // 将字节数组包装成ByteBuffer
      stringBuilder.append(byteBuffer.toString());
      byteBuffer.clear();
      bytesRead = in.read(buf);
    }
    System.out.println(stringBuilder.toString());

  }

  @Test
  public void testMain() throws Exception {
//    byte[] a = "test test".getBytes();
//    int i = 128;
//    byte b = (byte) i;
//    System.out.println(Integer.toBinaryString(256));
//    write(a);

//    DataOutputStream out = new DataOutputStream(new FileOutputStream(baseDirSpecified+"/a.txt"));
//    out.writeShort(122);
//    out.close();
  }


  static void write(int a){
    System.out.println(Integer.toBinaryString(a)+":"+a);
  }

  static void write(byte[] a){
    for (int i = 0 ; i < a.length ; i++) {
      write(a[i]);
    }
    write(-127);
  }
}
