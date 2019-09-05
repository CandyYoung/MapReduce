package com.zhanghq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        for (IntWritable temp : values) {
            max = Math.max(max, temp.get());
        }

        context.write(key, new IntWritable(max));
    }
}
