package com.zhanghq;

import com.zhanghq.db2dfs.Db2DfsMapper;
import com.zhanghq.dfs2db.Dfs2DbMapper;
import com.zhanghq.dfs2db.Dfs2DbReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DbDemoRunner extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();

        // 配置MySQL登入参数
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
        conf.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://master:3306/ncdc");
        conf.set(DBConfiguration.USERNAME_PROPERTY, "root");
        conf.set(DBConfiguration.PASSWORD_PROPERTY, "123456");

        Job job = Job.getInstance(conf);
        job.setJarByClass(DbDemoRunner.class);

        // 把MySQL连接驱动jar包加入到ClassPath
        job.addArchiveToClassPath(new Path("hdfs://master:9000/lib/mysql/mysql-connector-java-8.0.18.jar"));

        if ("1".equals(strings[1])) {
            job.setJobName("Dump dfs file content into mysql");
            job.setOutputFormatClass(DBOutputFormat.class);

            job.setMapperClass(Dfs2DbMapper.class);
            job.setReducerClass(Dfs2DbReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(10);

            // 设置输入参数及输出的表名、列名
            FileInputFormat.addInputPath(job, new Path(strings[0]));
            DBOutputFormat.setOutput(job, "weather", "USAF_station_id", "WBAN_station_id", "date",
                    "time", "latitude","longitude","elevation","wind_direction","wind_direction_quality_code",
                    "sky_ceiling_height","sky_ceiling_height_quality_code","visibility_distance","visibility_distance_quality_code",
                    "air_temperature","air_temperature_quality_code","dew_point_temperature","dew_point_temperature_quality_code",
                    "atmospheric_pressure","atmospheric_pressure_quality_code");
        } else if ("2".equals(strings[1])) {

            job.setJobName("Dump mysql table into hdfs file");
            job.setInputFormatClass(DBInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            DBInputFormat.setInput(job, WeatherDbBean.class, "weather", "", "_id",
                    "USAF_station_id", "WBAN_station_id", "date",
                    "time", "latitude", "longitude", "elevation", "wind_direction", "wind_direction_quality_code",
                    "sky_ceiling_height", "sky_ceiling_height_quality_code", "visibility_distance", "visibility_distance_quality_code",
                    "air_temperature", "air_temperature_quality_code", "dew_point_temperature", "dew_point_temperature_quality_code",
                    "atmospheric_pressure", "atmospheric_pressure_quality_code");
            TextOutputFormat.setOutputPath(job, new Path(strings[0]));

//            conf.set("mapreduce.output.textoutputformat.separator", "");
            job.getConfiguration().setInt("mapreduce.job.maps", 1);

            int map_count = job.getConfiguration().getInt("mapreduce.job.maps", 1);
            System.out.println("mapreduce.job.maps=" + map_count);

            job.setMapperClass(Db2DfsMapper.class);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DbDemoRunner <dfs file path> <work mode, 1 means dfs2db 2 means db2dfs>");
            System.exit(-1);
        }
        if(!"1".equals(args[1]) && !"2".equals(args[1])) {
            System.err.println("Invalid work mode.\r\nUsage: DbDemoRunner <dfs file path> <work mode, 1 means dfs2db 2 means db2dfs>");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new DbDemoRunner(), args);
        System.exit(exitCode);
    }
}
