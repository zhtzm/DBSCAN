package com.java;

import com.java.mapper.HDFS;
import com.java.mapper.Hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    private static String fileName = null;

    public static void main(String[] args) throws Exception {
        //参数：阈值, 核心最小数量, 本地文件地址, num_field, hasID
//        double threshold = Double.parseDouble(args[0]);
//        int minNum = Integer.parseInt(args[1]);
//        String jobName = args[2];
//        String localPath = args[3];
//        int num_field = Integer.parseInt(args[4]);
//        Boolean hasID = Boolean.parseBoolean(args[5]);
//
//        Main.uploadLocalFile(jobName, localPath, num_field, hasID);
        DataPartitioning.partitioning(fileName, 4);
    }

    private static void uploadLocalFile(String jobName, String localPath, Integer num_field, Boolean hasID)
            throws IOException, SQLException, ClassNotFoundException {
        HDFS hdfs = new HDFS();
        Hive hive = new Hive();

        String inputPath = HDFS.JOB_PATH + "/input";
        hdfs.init();
        hdfs.upload(localPath, inputPath);
        hdfs.close();

        int lastIndex = localPath.lastIndexOf("/");
        if (lastIndex != -1) {
            fileName = localPath.substring(lastIndex + 1);
        } else {
            throw new FileNotFoundException();
        }
        String hdfsPath = inputPath + "/" + fileName;

        hive.createTable(null, jobName, num_field, hasID);
        hive.loadData(null, jobName, hdfsPath);
    }
}