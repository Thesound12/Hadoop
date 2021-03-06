package com.bjfu.mapreduce.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 1.获取一行
         * 2.切割
         * 3.循环写出
         */
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word :
                words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
