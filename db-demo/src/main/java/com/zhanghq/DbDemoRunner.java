package com.zhanghq;

import com.zhanghq.dfs2db.Dfs2DbMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DbDemoRunner extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();

        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
        conf.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://master:3306/ncdc");
        conf.set(DBConfiguration.USERNAME_PROPERTY, "root");
        conf.set(DBConfiguration.PASSWORD_PROPERTY, "123456");

        Job job = Job.getInstance(conf, "Dump dfs file content into mysql");
        job.setJarByClass(DbDemoRunner.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        job.setMapperClass(Dfs2DbMapper.class);
        job.setNumReduceTasks(0);

        DBOutputFormat.setOutput(job, "weather", "USAF_station_id", "WBAN_station_id", "date",
                "time", "latitude","longitude","elevation","wind_direction","wind_direction_quality_code",
                "sky_ceiling_height","sky_ceiling_height_quality_code","visibility_distance","visibility_distance_quality_code",
                "air_temperature","air_temperature_quality_code","dew_point_temperature","dew_point_temperature_quality_code",
                "atmospheric_pressure","atmospheric_pressure_quality_code");

        FileInputFormat.addInputPath(job, new Path(strings[0]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: DbDemoRunner <input path>");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new DbDemoRunner(), args);
        System.exit(exitCode);
    }
}
