package com.zhanghq;

import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static int MISS_CODE = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        NcdcWeather ncdcWeather = new NcdcWeather(line);
        String year = ncdcWeather.getDate().substring(0, 4);
        int temperature = 0;
        if (ncdcWeather.getAir_temperature().startsWith("+")) {
            temperature = Integer.parseInt(ncdcWeather.getAir_temperature().substring(1));
        } else {
            temperature = Integer.parseInt(ncdcWeather.getAir_temperature());
        }


        if (temperature != MISS_CODE && ncdcWeather.getAir_temperature_quality_code().matches("[01459]")) {
            context.write(new Text(year), new IntWritable(temperature));
        }
    }
}
