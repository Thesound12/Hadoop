package com.bjfu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /**
         * 输入样例
         * atguigu,1
         * atguigu,1
         */
        /**
         * 1.累加求和
         * 2.写出
         */
        int sum = 0;
        for (IntWritable value :
                values) {
            sum += value.get();
        }
        v.set(sum);
        //写出：atguigu,2
        context.write(key, v);
    }
}
