package com.bjfu.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * public class WholeFileInputFormat extends FileInputFormat<Text, ByteWritable>，
 * 因为这里是<Text, ByteWritable>，所以这里的RecordReader也是<Text, ByteWritable>
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    FileSplit split;//因为读取的是文件，所以是File的Split。split是切牌你信息
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();//注意，这里是Bytes而不是Byte
    boolean isProgress = true;


    /**
     * @param inputSplit         从WholeFileInputFormat类中拿到
     * @param taskAttemptContext 从WholeFileInputFormat类中拿到
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        //核心业务逻辑处理：对key和value的封装
        /**
         * 1.获取fs对象，就可以拿到切牌你的信息，如a.txt、b.txt、c.txt。
         * 文件名称封装到Text中，value值封装到ByteWritable中
         * 2.获取输入流
         * 3.拷贝（先拷贝到缓存中）
         * 4.封装v
         * 5.封装k
         * 6.关闭资源
         */
        if (isProgress) {
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);//获取fs对象,然后进行IO流的操作

            byte[] buffer = new byte[(int) split.getLength()];
            FSDataInputStream fis = fileSystem.open(path);
            IOUtils.readFully(fis, buffer, 0, buffer.length);//将文件的内容读取到buffer中
            v.set(buffer, 0, buffer.length);
            k.set(path.toString());
            IOUtils.closeStream(fis);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        //key便是public class WholeRecordReader extends
        // RecordReader<Text, ByteWritable> {
        //的Text
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        //value便是public class WholeRecordReader extends
        // RecordReader<Text, ByteWritable> {
        //的ByteWritable
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        //这里相当于进度条
        return 0;
    }

    @Override
    public void close() throws IOException {
        //在其他方法中关闭，也就在此处用不着关闭
    }
}
