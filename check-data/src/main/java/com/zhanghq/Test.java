package com.zhanghq;


import com.zhanghq.bean.NcdcWeather;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Test {
    public static void main(String[] args) {
        // write your code here
        System.out.println(System.getProperty("user.dir"));
        String path = Test.class.getClassLoader().getResource("raw/1901").getPath();
        System.out.println(path);
        File tetFile = new File(path);
        if (tetFile.exists()) {
            FileReader fileReader = null;
            BufferedReader bufferedReader = null;
            try {
                fileReader = new FileReader(tetFile);
                bufferedReader = new BufferedReader(fileReader);
                String line = bufferedReader.readLine();
                System.out.println(line);

                NcdcWeather ncdcDataParser = new NcdcWeather(line);
                System.out.println(ncdcDataParser.toString());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(fileReader);
                IOUtils.closeQuietly(bufferedReader);
            }

        } else {
            System.out.println("Can not find file");
        }
    }
}
