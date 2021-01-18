package com.bjfu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        /*
          1.获取HDFS客户端对象
         */
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://hadoop102:9000");//core-site.xml


        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        /*
          2.在HDFS上创建路径
         */
        fileSystem.mkdirs(new Path("/0115/fei"));
        /*
        关闭资源
         */
        fileSystem.close();
        System.out.println("over");
    }

    //1.文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        /*
          1.获取HDFS客户端对象fs
          2.执行上传API
          3.关闭资源
         */
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        fileSystem.copyFromLocalFile(new Path("D:/aad.txt"), new Path("/aad.txt"));
        fileSystem.close();
    }

    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        fileSystem.copyToLocalFile(false, new Path("/aad.txt"), new Path("C:\\Users\\Thesound\\Desktop"), true);
        fileSystem.close();
    }

    /*
     * HDFS 文件详情查看
     * 查看文件名称、权限、长度、块信息
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/aaa.txt"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            System.out.println(Arrays.toString(fileStatus.getBlockLocations()));
        }
        fileSystem.close();
    }
    /*
     *HDFS 文件和文件夹判断   略
     */


}
