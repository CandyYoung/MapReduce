package com.zhanghq.db2dfs;

import com.zhanghq.WeatherDbBean;
import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Db2DfsMapper extends Mapper<LongWritable, WeatherDbBean, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, WeatherDbBean value, Context context) throws IOException, InterruptedException {
        NcdcWeather weather = value.getNcdcWeather();

        String line = String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
                weather.getUSAF_station_id(), weather.getWBAN_station_id(),
                weather.getDate(), weather.getTime(),
                weather.getLatitude(), weather.getLongitude(), weather.getElevation(),
                weather.getWind_direction(), weather.getWind_direction_quality_code(),
                weather.getSky_ceiling_height(), weather.getSky_ceiling_height_quality_code(),
                weather.getVisibility_distance(), weather.getVisibility_distance_quality_code(),
                weather.getAir_temperature(), weather.getAir_temperature_quality_code(),
                weather.getDew_point_temperature(), weather.getDew_point_temperature_quality_code(),
                weather.getAtmospheric_pressure(), weather.getAtmospheric_pressure_quality_code()
        );

        context.write(null, new Text(line));
    }
}
