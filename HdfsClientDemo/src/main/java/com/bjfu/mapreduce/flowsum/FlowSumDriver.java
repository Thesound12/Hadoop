package com.bjfu.mapreduce.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 1.获取job对象
         * 2.设置jar的路径
         * 3.关联mapper和reducer
         * 4.设置mapper输出的key和value的类型
         * 5.设置最终输出的key和value的类型
         * 6.设置输入输出路径
         * 7.提交job
         */

        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar的路径
        job.setJarByClass(FlowSumDriver.class);
        //关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //设置mapper输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\MyHadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyHadoop\\output"));
        //提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
