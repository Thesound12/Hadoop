package com.bjfu.mapreduce.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        context.write(key, v);
    }
}
