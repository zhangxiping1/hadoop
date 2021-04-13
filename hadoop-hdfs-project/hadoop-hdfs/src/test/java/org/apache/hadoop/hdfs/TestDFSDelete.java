package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestDFSDelete {


    static void createFile(FileSystem fs, Path f) throws IOException {
        DataOutputStream a_out = fs.create(f);
        a_out.writeBytes("something");
        a_out.close();
    }

    @Test
    public void testDelete() throws Exception {
        Configuration conf = new HdfsConfiguration();
        conf.set("fs.trash.interval","5");
        conf.set("fs.trash.checkpoint.interval","1");
        System.setProperty("HADOOP_USER_NAME","zhangxiping");
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).nameNodeHttpPort(50070).build();

        try {
            FileSystem fs = cluster.getFileSystem();

            { //case 1
                createFile(fs,new Path("/user/zhangxiping/zhang.txt"));
                assertTrue(fs.delete(new Path("/user/zhangxiping/zhang.txt"), true));
            }

            { //case 2
                createFile(fs,new Path("/user/zhang.txt"));
                assertTrue(fs.delete(new Path("/user/zhang.txt"), true));

                createFile(fs,new Path("/user/zhang.txt"));
                assertTrue(fs.delete(new Path("/user/zhang.txt"), true));
            }

            { //case 3
                fs.mkdirs(new Path("/user/zhang123"));
                createFile(fs,new Path("/user/zhang123/zhang.txt"));
                assertTrue(fs.delete(new Path("/user/zhang123"), true));
                fs.mkdirs(new Path("/user/zhang123"));
                assertTrue(fs.delete(new Path("/user/zhang123"), true));
            }

            { //case 4
                assertTrue(fs.delete(new Path("/user/zhangxiping/.Trash/Current/zhangxiping/user/zhang.txt"), true));
            }
        } finally {
            //Thread.sleep(200000);
            if (cluster != null) {cluster.shutdown();}
        }
    }

}