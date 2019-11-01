package com.zhanghq.bean;

import com.zhanghq.utils.StringUtils;

import java.util.Objects;

public class NcdcWeather {
    private String USAF_station_id;
    private String WBAN_station_id;
    private String date;
    private String time;
    private String latitude;
    private String longitude;
    /** 海拔*/
    private String elevation;
    /** 风向*/
    private String wind_direction;
    private String wind_direction_quality_code;
    private String sky_ceiling_height;
    private String sky_ceiling_height_quality_code;
    private String visibility_distance;
    private String visibility_distance_quality_code;
    private String air_temperature;
    private String air_temperature_quality_code;
    private String dew_point_temperature;
    private String dew_point_temperature_quality_code;
    private String atmospheric_pressure;
    private String atmospheric_pressure_quality_code;

    public NcdcWeather(String rowData) {
        if (StringUtils.isEmpty(rowData) || rowData.length() < 105) {
            return;
        }

        USAF_station_id = rowData.substring(4, 10);
        WBAN_station_id = rowData.substring(10, 15);
        date = rowData.substring(15, 23);
        time = rowData.substring(23, 27);
        latitude = rowData.substring(28, 34);
        longitude = rowData.substring(34, 41);
        elevation = rowData.substring(46, 51);
        wind_direction = rowData.substring(60, 63);
        wind_direction_quality_code = rowData.substring(63, 64);
        sky_ceiling_height = rowData.substring(70, 75);
        sky_ceiling_height_quality_code = rowData.substring(75, 76);
        visibility_distance = rowData.substring(78, 84);
        visibility_distance_quality_code = rowData.substring(84, 85);
        air_temperature = rowData.substring(87, 92);
        air_temperature_quality_code = rowData.substring(92, 93);
        dew_point_temperature = rowData.substring(93, 98);
        dew_point_temperature_quality_code = rowData.substring(98, 99);
        atmospheric_pressure = rowData.substring(99, 104);
        atmospheric_pressure_quality_code = rowData.substring(104, 105);
    }

    public String getUSAF_station_id() {
        return USAF_station_id;
    }

    public void setUSAF_station_id(String USAF_station_id) {
        this.USAF_station_id = USAF_station_id;
    }

    public String getWBAN_station_id() {
        return WBAN_station_id;
    }

    public void setWBAN_station_id(String WBAN_station_id) {
        this.WBAN_station_id = WBAN_station_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getElevation() {
        return elevation;
    }

    public void setElevation(String elevation) {
        this.elevation = elevation;
    }

    public String getWind_direction() {
        return wind_direction;
    }

    public void setWind_direction(String wind_direction) {
        this.wind_direction = wind_direction;
    }

    public String getWind_direction_quality_code() {
        return wind_direction_quality_code;
    }

    public void setWind_direction_quality_code(String wind_direction_quality_code) {
        this.wind_direction_quality_code = wind_direction_quality_code;
    }

    public String getSky_ceiling_height() {
        return sky_ceiling_height;
    }

    public void setSky_ceiling_height(String sky_ceiling_height) {
        this.sky_ceiling_height = sky_ceiling_height;
    }

    public String getSky_ceiling_height_quality_code() {
        return sky_ceiling_height_quality_code;
    }

    public void setSky_ceiling_height_quality_code(String sky_ceiling_height_quality_code) {
        this.sky_ceiling_height_quality_code = sky_ceiling_height_quality_code;
    }

    public String getVisibility_distance() {
        return visibility_distance;
    }

    public void setVisibility_distance(String visibility_distance) {
        this.visibility_distance = visibility_distance;
    }

    public String getVisibility_distance_quality_code() {
        return visibility_distance_quality_code;
    }

    public void setVisibility_distance_quality_code(String visibility_distance_quality_code) {
        this.visibility_distance_quality_code = visibility_distance_quality_code;
    }

    public String getAir_temperature() {
        return air_temperature;
    }

    public void setAir_temperature(String air_temperature) {
        this.air_temperature = air_temperature;
    }

    public String getAir_temperature_quality_code() {
        return air_temperature_quality_code;
    }

    public void setAir_temperature_quality_code(String air_temperature_quality_code) {
        this.air_temperature_quality_code = air_temperature_quality_code;
    }

    public String getDew_point_temperature() {
        return dew_point_temperature;
    }

    public void setDew_point_temperature(String dew_point_temperature) {
        this.dew_point_temperature = dew_point_temperature;
    }

    public String getDew_point_temperature_quality_code() {
        return dew_point_temperature_quality_code;
    }

    public void setDew_point_temperature_quality_code(String dew_point_temperature_quality_code) {
        this.dew_point_temperature_quality_code = dew_point_temperature_quality_code;
    }

    public String getAtmospheric_pressure() {
        return atmospheric_pressure;
    }

    public void setAtmospheric_pressure(String atmospheric_pressure) {
        this.atmospheric_pressure = atmospheric_pressure;
    }

    public String getAtmospheric_pressure_quality_code() {
        return atmospheric_pressure_quality_code;
    }

    public void setAtmospheric_pressure_quality_code(String atmospheric_pressure_quality_code) {
        this.atmospheric_pressure_quality_code = atmospheric_pressure_quality_code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NcdcWeather that = (NcdcWeather) o;
        return Objects.equals(USAF_station_id, that.USAF_station_id) &&
                Objects.equals(WBAN_station_id, that.WBAN_station_id) &&
                Objects.equals(date, that.date) &&
                Objects.equals(time, that.time) &&
                Objects.equals(latitude, that.latitude) &&
                Objects.equals(longitude, that.longitude) &&
                Objects.equals(elevation, that.elevation) &&
                Objects.equals(wind_direction, that.wind_direction) &&
                Objects.equals(wind_direction_quality_code, that.wind_direction_quality_code) &&
                Objects.equals(sky_ceiling_height, that.sky_ceiling_height) &&
                Objects.equals(sky_ceiling_height_quality_code, that.sky_ceiling_height_quality_code) &&
                Objects.equals(visibility_distance, that.visibility_distance) &&
                Objects.equals(visibility_distance_quality_code, that.visibility_distance_quality_code) &&
                Objects.equals(air_temperature, that.air_temperature) &&
                Objects.equals(air_temperature_quality_code, that.air_temperature_quality_code) &&
                Objects.equals(dew_point_temperature, that.dew_point_temperature) &&
                Objects.equals(dew_point_temperature_quality_code, that.dew_point_temperature_quality_code) &&
                Objects.equals(atmospheric_pressure, that.atmospheric_pressure) &&
                Objects.equals(atmospheric_pressure_quality_code, that.atmospheric_pressure_quality_code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(USAF_station_id, WBAN_station_id, date, time, latitude, longitude, elevation, wind_direction, wind_direction_quality_code, sky_ceiling_height, sky_ceiling_height_quality_code, visibility_distance, visibility_distance_quality_code, air_temperature, air_temperature_quality_code, dew_point_temperature, dew_point_temperature_quality_code, atmospheric_pressure, atmospheric_pressure_quality_code);
    }

    @Override
    public String toString() {
        return "NcdcWeather{" +
                "USAF_station_id='" + USAF_station_id + '\'' +
                ", WBAN_station_id='" + WBAN_station_id + '\'' +
                ", date='" + date + '\'' +
                ", time='" + time + '\'' +
                ", latitude='" + latitude + '\'' +
                ", longitude='" + longitude + '\'' +
                ", elevation='" + elevation + '\'' +
                ", wind_direction='" + wind_direction + '\'' +
                ", wind_direction_quality_code='" + wind_direction_quality_code + '\'' +
                ", sky_ceiling_height='" + sky_ceiling_height + '\'' +
                ", sky_ceiling_height_quality_code='" + sky_ceiling_height_quality_code + '\'' +
                ", visibility_distance='" + visibility_distance + '\'' +
                ", visibility_distance_quality_code='" + visibility_distance_quality_code + '\'' +
                ", air_temperature='" + air_temperature + '\'' +
                ", air_temperature_quality_code='" + air_temperature_quality_code + '\'' +
                ", dew_point_temperature='" + dew_point_temperature + '\'' +
                ", dew_point_temperature_quality_code='" + dew_point_temperature_quality_code + '\'' +
                ", atmospheric_pressure='" + atmospheric_pressure + '\'' +
                ", atmospheric_pressure_quality_code='" + atmospheric_pressure_quality_code + '\'' +
                '}';
    }
}
