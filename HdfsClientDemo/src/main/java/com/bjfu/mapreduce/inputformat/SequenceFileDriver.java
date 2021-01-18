package com.bjfu.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        // 1 获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置 jar 包存储位置、关联自定义的 mapper 和 reducer
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        // 7 设置输入的 inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 8 设置输出的 outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 3 设置 map 输出端的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // 4 设置最终输出端的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\MyHadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\MyHadoop\\output"));
        // 6 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
