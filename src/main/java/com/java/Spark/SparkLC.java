package com.java.Spark;

import com.java.DBSCAN;
import com.java.ds.CType;
import com.java.ds.PType;
import com.java.ds.Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkLC {
    public static void clustering(int minPts, double epsilon, String partitionFile) {
        SparkConf conf = new SparkConf().setAppName("Local Clustering");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "hdfs://centos01:9000/job/DBSCAN/output/" + partitionFile + "_partition.txt";
        JavaRDD<String> input = sc.textFile(path);

        JavaPairRDD<String, String> pairs = input.mapToPair(line -> {
            String[] parts = line.split("\t");
            return new Tuple2<>(parts[0], parts[2] + " " + parts[1]);
        });

        JavaPairRDD<String, Iterable<String>> groupedPairs = pairs.groupByKey();

        JavaRDD<String> results = groupedPairs.flatMap(tuple -> {
            List<String> output = new ArrayList<>();
            List<Point> pointList = new ArrayList<>();
            String partitionId = tuple._1;

            for (String line : tuple._2) {
                Point p = new Point(line, Integer.parseInt(partitionId));
                p.setPartitionId(Integer.parseInt(partitionId));
                pointList.add(p);
            }

            // 创建 DBSCAN 实例并运行
            DBSCAN dbscan = new DBSCAN(pointList);
            dbscan.RunDBSCAN(minPts, epsilon);

            for (Point point : pointList) {
                String pointFlag;
                if (point.getCt() == CType.CORE) {
                    pointFlag = "C";
                } else if (point.getCt() == CType.BORDER) {
                    pointFlag = "B";
                } else {
                    pointFlag = "N";
                }

                String record = partitionId + "\t" + point.getClusterId() + "\tFP\t" + pointFlag + "\t" + point.getId();
                output.add(record);

                if (pointFlag.equals("C") && point.getPType() == PType.IM) {
                    record = partitionId + "\t" + point.getClusterId() + "\tAP\t" + pointFlag + "\t" + point.getId();
                    output.add(record);
                }

                if ((pointFlag.equals("C") || pointFlag.equals("B")) && point.getPType() == PType.OM) {
                    record = partitionId + "\t" + point.getClusterId() + "\tBP\t" + pointFlag + "\t" + point.getId();
                    output.add(record);
                }
            }

            return output.iterator();
        });

        results.saveAsTextFile("/job/DBSCAN/output/" + partitionFile + "_local");

        sc.close();
    }
}