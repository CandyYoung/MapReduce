package com.zhanghq.dfs2db;

import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Dfs2DbMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        NcdcWeather ncdcWeather = new NcdcWeather(value.toString());
        context.write(new Text(ncdcWeather.getDate().substring(0, 4)), value);
    }
}
