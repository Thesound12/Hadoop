package com.bjfu.mapreduce.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 1.获取job对象
         * 2.设置jar的存储路径
         * 3.管理mapper和reducer类
         * 4.设置mapper的输出的key和value的类型
         * 5.设置最终输出的key和value的类型
         * 6.设置输入输出路径
         * 7.提交job
         */
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");//+++++++
        Job job = Job.getInstance(conf);
        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);//+++++++
        FileInputFormat.setInputPaths(job, new Path("D:\\MyHadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyHadoop\\output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
