package com.bjfu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {
    /**
     * 接下来是IO流的操作，是定制化的需求。
     * FS要放到HDFS上，所以管输出，就是输出流
     */

    /**
     * 步骤：
     * 1.获取对象
     * 2.获取输入流
     * 3.获取输出流
     * 4.流的对拷
     * 5.关闭资源
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        FileInputStream fileInputStream = new FileInputStream(new File("C:\\Users\\Thesound\\Desktop\\student.jpg"));

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/student.jpg"));

        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, conf);
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fileSystem.close();
    }

    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/aac.txt"));

        FileOutputStream fileOutputStream = new FileOutputStream("D:\\aar.txt");
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, conf);
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
        fileSystem.close();
    }
    /**
     * 定位文件读取
     * 1．需求：分块读取 HDFS 上的大文件，比如根目录下的/hadoop-2.7.2.tar.gz 略
     */
}
