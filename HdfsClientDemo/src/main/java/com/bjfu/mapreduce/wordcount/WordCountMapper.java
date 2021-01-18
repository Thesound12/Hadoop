package com.bjfu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map阶段
 * LongWritable：输入数据的key（字节的偏移量）
 * Text：输入数据的value
 * Text：输出数据的key的类型，如atguigu
 * IntWritable：输出数据的value的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 1.获取一行数据
         * 2.切割单词
         * 3.循环写出
         */
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word :
                words) {
            k.set(word);//结果：atguigu
            context.write(k, v);
        }
    }
}
