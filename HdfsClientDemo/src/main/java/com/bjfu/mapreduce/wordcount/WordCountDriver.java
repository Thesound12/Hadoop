package com.bjfu.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 1.获取job对象，与之前的HDFS中的FS类似
         * 2.设置jar包的存放位置
         * 3.关联Map和Reduce
         * 4.设置Mapper阶段输出数据的key和value类型
         * 5.设置最终输出数据的key和value类型
         * 6.设置输入路径和输出路径
         * 7.提交job
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置 4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        FileInputFormat.setInputPaths(job, new Path("D:\\MyHadoop\\input"));//设置输入路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyHadoop\\output"));
        //job.submit();
        boolean b = job.waitForCompletion(true);//true的时候就打印出提示信息
        System.exit(b ? 0 : 1);//这个可以不写
    }
}
