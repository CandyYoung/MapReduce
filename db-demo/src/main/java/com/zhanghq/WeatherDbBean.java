package com.zhanghq;

import com.zhanghq.bean.NcdcWeather;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WeatherDbBean implements DBWritable, Writable {
    private NcdcWeather ncdcWeather;

    public WeatherDbBean(NcdcWeather ncdcWeather) {
        this.ncdcWeather = ncdcWeather;
    }

    public NcdcWeather getNcdcWeather() {
        return ncdcWeather;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        int index = 1;
        preparedStatement.setString(index++, ncdcWeather.getUSAF_station_id());
        preparedStatement.setString(index++, ncdcWeather.getWBAN_station_id());
        preparedStatement.setString(index++, ncdcWeather.getDate());
        preparedStatement.setString(index++, ncdcWeather.getTime());
        preparedStatement.setString(index++, ncdcWeather.getLatitude());
        preparedStatement.setString(index++, ncdcWeather.getLongitude());
        preparedStatement.setString(index++, ncdcWeather.getElevation());
        preparedStatement.setString(index++, ncdcWeather.getWind_direction());
        preparedStatement.setString(index++, ncdcWeather.getWind_direction_quality_code());
        preparedStatement.setString(index++, ncdcWeather.getSky_ceiling_height());
        preparedStatement.setString(index++, ncdcWeather.getSky_ceiling_height_quality_code());
        preparedStatement.setString(index++, ncdcWeather.getVisibility_distance());
        preparedStatement.setString(index++, ncdcWeather.getVisibility_distance_quality_code());
        preparedStatement.setString(index++, ncdcWeather.getAir_temperature());
        preparedStatement.setString(index++, ncdcWeather.getAir_temperature_quality_code());
        preparedStatement.setString(index++, ncdcWeather.getDew_point_temperature());
        preparedStatement.setString(index++, ncdcWeather.getDew_point_temperature_quality_code());
        preparedStatement.setString(index++, ncdcWeather.getAtmospheric_pressure());
        preparedStatement.setString(index++, ncdcWeather.getAtmospheric_pressure_quality_code());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        int index = 1;
        ncdcWeather.setUSAF_station_id(resultSet.getString(index++));
        ncdcWeather.setWBAN_station_id(resultSet.getString(index++));
        ncdcWeather.setDate(resultSet.getString(index++));
        ncdcWeather.setTime(resultSet.getString(index++));
        ncdcWeather.setLatitude(resultSet.getString(index++));
        ncdcWeather.setLongitude(resultSet.getString(index++));
        ncdcWeather.setElevation(resultSet.getString(index++));
        ncdcWeather.setWind_direction(resultSet.getString(index++));
        ncdcWeather.setWind_direction_quality_code(resultSet.getString(index++));
        ncdcWeather.setSky_ceiling_height(resultSet.getString(index++));
        ncdcWeather.setSky_ceiling_height_quality_code(resultSet.getString(index++));
        ncdcWeather.setVisibility_distance(resultSet.getString(index++));
        ncdcWeather.setVisibility_distance_quality_code(resultSet.getString(index++));
        ncdcWeather.setAir_temperature(resultSet.getString(index++));
        ncdcWeather.setAir_temperature_quality_code(resultSet.getString(index++));
        ncdcWeather.setDew_point_temperature(resultSet.getString(index++));
        ncdcWeather.setDew_point_temperature_quality_code(resultSet.getString(index++));
        ncdcWeather.setAtmospheric_pressure(resultSet.getString(index++));
        ncdcWeather.setAtmospheric_pressure_quality_code(resultSet.getString(index++));
    }

//    @Override
//    public int compareTo(WeatherDbBean o) {
//        Text.Comparator comp = new Text.Comparator();
//        comp.compare
//        RawComparator<NcdcWeather> comparator = new RawComparator<NcdcWeather>() {
//            @Override
//            public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
//                return 0;
//            }
//
//            @Override
//            public int compare(NcdcWeather o1, NcdcWeather o2) {
//                return 0;
//            }
//        }
//        return ncdcWeather < o ? -1 : (ncdcWeather.equals(o) ? 0 : 1);
//    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ncdcWeather.getUSAF_station_id());
        dataOutput.writeUTF(ncdcWeather.getWBAN_station_id());
        dataOutput.writeUTF(ncdcWeather.getDate());
        dataOutput.writeUTF(ncdcWeather.getTime());
        dataOutput.writeUTF(ncdcWeather.getLatitude());
        dataOutput.writeUTF(ncdcWeather.getLongitude());
        dataOutput.writeUTF(ncdcWeather.getElevation());
        dataOutput.writeUTF(ncdcWeather.getWind_direction());
        dataOutput.writeUTF(ncdcWeather.getWind_direction_quality_code());
        dataOutput.writeUTF(ncdcWeather.getSky_ceiling_height());
        dataOutput.writeUTF(ncdcWeather.getSky_ceiling_height_quality_code());
        dataOutput.writeUTF(ncdcWeather.getVisibility_distance());
        dataOutput.writeUTF(ncdcWeather.getVisibility_distance_quality_code());
        dataOutput.writeUTF(ncdcWeather.getAir_temperature());
        dataOutput.writeUTF(ncdcWeather.getAir_temperature_quality_code());
        dataOutput.writeUTF(ncdcWeather.getDew_point_temperature());
        dataOutput.writeUTF(ncdcWeather.getDew_point_temperature_quality_code());
        dataOutput.writeUTF(ncdcWeather.getAtmospheric_pressure());
        dataOutput.writeUTF(ncdcWeather.getAtmospheric_pressure_quality_code());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ncdcWeather.setUSAF_station_id(dataInput.readUTF());
        ncdcWeather.setWBAN_station_id(dataInput.readUTF());
        ncdcWeather.setDate(dataInput.readUTF());
        ncdcWeather.setTime(dataInput.readUTF());
        ncdcWeather.setLatitude(dataInput.readUTF());
        ncdcWeather.setLongitude(dataInput.readUTF());
        ncdcWeather.setElevation(dataInput.readUTF());
        ncdcWeather.setWind_direction(dataInput.readUTF());
        ncdcWeather.setWind_direction_quality_code(dataInput.readUTF());
        ncdcWeather.setSky_ceiling_height(dataInput.readUTF());
        ncdcWeather.setSky_ceiling_height_quality_code(dataInput.readUTF());
        ncdcWeather.setVisibility_distance(dataInput.readUTF());
        ncdcWeather.setVisibility_distance_quality_code(dataInput.readUTF());
        ncdcWeather.setAir_temperature(dataInput.readUTF());
        ncdcWeather.setAir_temperature_quality_code(dataInput.readUTF());
        ncdcWeather.setDew_point_temperature(dataInput.readUTF());
        ncdcWeather.setDew_point_temperature_quality_code(dataInput.readUTF());
        ncdcWeather.setAtmospheric_pressure(dataInput.readUTF());
        ncdcWeather.setAtmospheric_pressure_quality_code(dataInput.readUTF());
    }
}
