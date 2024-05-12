package com.java;

import com.java.MR.LocalClustering;
import com.java.mapper.HDFS;
import com.java.mapper.Hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MR_DBSCAN {
    public static DateTimeFormatter Formatter
            = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ");
    public static final HDFS hdfs = new HDFS();
    public static final Hive hive = new Hive();

    public static String TableName = null;
    public static int MinPts;
    public static double Epsilon;
    public static int NumField;
    public static int PartitionNum;
    public static String OutputPath;

    public static void main(String[] args) throws Exception {
        //参数：Epsilon, MinPts, localPath, NumField, PartitionNum
        Epsilon = Double.parseDouble(args[0]);
        MinPts = Integer.parseInt(args[1]);
        String localPath = args[2];
        NumField = Integer.parseInt(args[3]);
        PartitionNum = Integer.parseInt(args[4]);

        uploadLocalFile(localPath, NumField);
        DataPartitioning.partitioning(PartitionNum, Epsilon, NumField);
        LocalClustering.clustering(MinPts, Epsilon, TableName);
        Merging.merging(TableName, NumField);
        Draw.draw(NumField, OutputPath);
    }

    public static void uploadLocalFile(String localPath, Integer num_field)
            throws IOException, SQLException, ClassNotFoundException {
        String inputPath = HDFS.JOB_PATH + "/input";
        hdfs.init();
        System.out.println(Formatter.format(LocalDateTime.now()) + "开始上传文件");
        hdfs.upload(localPath, inputPath);
        System.out.println(Formatter.format(LocalDateTime.now()) + "上传文件成功");
        hdfs.close();

        int lastIndex = localPath.lastIndexOf("/");
        if (lastIndex != -1) {
            String fileName = localPath.substring(lastIndex + 1);
            String hdfsPath = inputPath + "/" + fileName;

            TableName = fileName.split("\\.")[0];
            System.out.println(Formatter.format(LocalDateTime.now()) + "开始建表：" + TableName);
            if(hive.existsTable(null, TableName)) {
                hive.dropTable(null, TableName);
                Hive.destroy();
            }
            hive.createTable(null, TableName, num_field, " ");
            System.out.println(Formatter.format(LocalDateTime.now()) + "建表成功");

            System.out.println(Formatter.format(LocalDateTime.now()) + "开始导入数据");
            hive.loadData(null, TableName, hdfsPath);
            System.out.println(Formatter.format(LocalDateTime.now()) + "导入数据完成");
        } else {
            throw new FileNotFoundException();
        }
    }
}