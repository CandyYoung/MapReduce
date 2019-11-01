package com.zhanghq.dfs2db;

import com.zhanghq.WeatherDbBean;
import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.util.TextUtils;

import java.io.IOException;

public class Dfs2DbMapper extends Mapper<LongWritable, Text, WeatherDbBean, Object> {

    enum Temperature {
        BELOW_ZERO,
        ABOVE_THIRTY,
        MALFORMED,
        MISS
    }

    enum StationIdMiss {
        USAF_ID_MISS,
        WBAN_ID_MISS
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        NcdcWeather ncdcWeather = new NcdcWeather(line);
        if (TextUtils.isEmpty(ncdcWeather.getUSAF_station_id())) {
            context.getCounter(StationIdMiss.USAF_ID_MISS).increment(1);
        }
        if (TextUtils.isEmpty(ncdcWeather.getWBAN_station_id())) {
            context.getCounter(StationIdMiss.WBAN_ID_MISS).increment(1);
        }
        if (TextUtils.isEmpty(ncdcWeather.getAir_temperature())) {
            context.getCounter(Temperature.MISS).increment(1);
        } else {
            try {
                long airTemperature = Long.parseLong(ncdcWeather.getAir_temperature());
                if (airTemperature < 0) {
                    context.getCounter(Temperature.BELOW_ZERO).increment(1);
                }
                if (airTemperature > 30) {
                    context.getCounter(Temperature.ABOVE_THIRTY).increment(1);
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
                context.getCounter(Temperature.MALFORMED).increment(1);
            }
        }

        context.write(new WeatherDbBean(ncdcWeather), null);
    }
}
