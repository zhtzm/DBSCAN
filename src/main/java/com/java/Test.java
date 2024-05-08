package com.java;

import com.java.mapper.HDFS;
import com.java.mapper.Hive;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class Test {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Hive hive = new Hive();
        HDFS hdfs = new HDFS();

        // hive.showTables(null);
        // farmdata onewdata pamap2

        ResultSet result = hive.sql(null, "SELECT * FROM farmdata limit 10");
        String outputPath = HDFS.JOB_PATH + "/output";
        Path file = new Path(outputPath + "/" + "tmp.txt");

        hdfs.init();
        FSDataOutputStream outputStream = hdfs.createFile(file);
        while (result.next()) {
            String value = result.getString(2);
            System.out.println(value);
            outputStream.writeBytes(value+'\n');
        }
        hdfs.close();
        System.out.println("Data written to HDFS successfully.");
    }
}
