package com.zhanghq.dfs2db;

import com.zhanghq.WeatherDbBean;
import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.util.TextUtils;

import java.io.IOException;

public class Dfs2DbReducer extends Reducer<LongWritable, Text, WeatherDbBean, Object> {

    enum Temperature {
        BELOW_MINUS_FIFTEEN,
        ABOVE_THIRTY_FIVE,
        MALFORMED,
        MISS
    }

    enum StationIdMiss {
        USAF_ID_MISS,
        WBAN_ID_MISS
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        while (values.iterator().hasNext()) {
            String line = values.iterator().next().toString();
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
                    // 温度数据被放大了10倍存储，这里比较时也相应乘10
                    String weatherStr = ncdcWeather.getAir_temperature();
                    if (weatherStr.startsWith("+")) {
                        weatherStr = weatherStr.substring(1);
                    }
                    long airTemperature = Long.parseLong(weatherStr);
                    if (airTemperature < -150) {
                        context.getCounter(Temperature.BELOW_MINUS_FIFTEEN).increment(1);
                    }
                    if (airTemperature > 350) {
                        context.getCounter(Temperature.ABOVE_THIRTY_FIVE).increment(1);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    context.getCounter(Temperature.MALFORMED).increment(1);
                }
            }

            context.write(new WeatherDbBean(ncdcWeather), null);
        }
    }
}
