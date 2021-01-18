package com.bjfu.mapreduce.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        /**
         * 传过来的是<a.txt:...> <b.txt:...> <c.txt:...>
         */
        //循环写出
        for (BytesWritable value :
                values) {
            context.write(key, value);
        }
    }
}
