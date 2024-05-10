package com.java;

import com.java.mapper.HDFS;
import com.java.mapper.Hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MR_DBSCAN {
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ");
    private static String fileName = null;
    public static String tableName = null;

    public static void main(String[] args) throws Exception {
        //参数：阈值, 核心最小数量, 本地文件地址, num_field
        double threshold = Double.parseDouble(args[0]);
        int minNum = Integer.parseInt(args[1]);
        String localPath = args[2];
        int num_field = Integer.parseInt(args[4]);

        MR_DBSCAN.uploadLocalFile(localPath, num_field);
        new DataPartitioning(fileName, 4, threshold).partitioning(50000);
    }

    public static void uploadLocalFile(String localPath, Integer num_field)
            throws IOException, SQLException, ClassNotFoundException {
        HDFS hdfs = new HDFS();
        Hive hive = new Hive();

        String inputPath = HDFS.JOB_PATH + "/input";
        hdfs.init();
        System.out.println(formatter.format(LocalDateTime.now()) + "开始上传文件");
        hdfs.upload(localPath, inputPath);
        System.out.println(formatter.format(LocalDateTime.now()) + "上传文件成功");
        hdfs.close();

        int lastIndex = localPath.lastIndexOf("/");
        if (lastIndex != -1) {
            fileName = localPath.substring(lastIndex + 1);
        } else {
            throw new FileNotFoundException();
        }
        String hdfsPath = inputPath + "/" + fileName;

        tableName = fileName.split("\\.")[0];
        System.out.println(formatter.format(LocalDateTime.now()) + "开始建表：" + tableName);
        hive.createTable(null, tableName, num_field, " ");
        System.out.println(formatter.format(LocalDateTime.now()) + "建表成功");
        System.out.println(formatter.format(LocalDateTime.now()) + "开始导入数据");
        hive.loadData(null, tableName, hdfsPath);
        System.out.println(formatter.format(LocalDateTime.now()) + "导入数据完成");
    }
}