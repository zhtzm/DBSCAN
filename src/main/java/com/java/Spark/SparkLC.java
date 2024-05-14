package com.java.Spark;

import com.java.DBSCAN;
import com.java.ds.CType;
import com.java.ds.PType;
import com.java.ds.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SparkLC {
    public static int MinPts;
    public static double Epsilon;

    public static void clustering(int minPts, double epsilon, String partitionFile) {
        MinPts = minPts;
        Epsilon = epsilon;

        SparkSession spark = SparkSession.builder().appName("Local Clustering").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> input = jsc.textFile("/job/DBSCAN/output/" + partitionFile + "_partition.txt");

        JavaPairRDD<String, String> pairs = input.mapToPair((PairFunction<String, String, String>) line -> {
            String[] parts = line.split("\t");
            if (parts.length == 3) {
                return new Tuple2<>(parts[0], parts[2] + " " + parts[1]);
            }
            return null;
        }).filter(Objects::nonNull);

        JavaPairRDD<String, Iterable<String>> groupedPairs = pairs.groupByKey();

        JavaRDD<String> results = groupedPairs.flatMap((FlatMapFunction<Tuple2<String, Iterable<String>>, String>) tuple -> {
            List<String> output = new ArrayList<>();
            List<Point> pointList = new ArrayList<>();
            String partitionId = tuple._1;

            for (String line : tuple._2) {
                Point p = new Point(line, Integer.valueOf(partitionId));
                p.setPartitionId(Integer.parseInt(partitionId));
                pointList.add(p);
            }

            new DBSCAN(pointList).RunDBSCAN(MinPts, Epsilon);

            for (Point point : pointList) {
                StringBuilder record = new StringBuilder();
                String pointFlag;
                if (point.getCt() == CType.CORE) {
                    pointFlag = "C";
                } else if (point.getCt() == CType.BORDER) {
                    pointFlag = "B";
                } else {
                    pointFlag = "N";
                }

                record.append(partitionId)
                        .append("\t")
                        .append(point.getClusterId())
                        .append("\tFP\t")
                        .append(pointFlag)
                        .append("\t")
                        .append(point.getId());

                output.add(record.toString());

                if (pointFlag.equals("C") && point.getPType() == PType.IM) {
                    record.setLength(0);
                    record.append(partitionId)
                            .append("\t")
                            .append(point.getClusterId())
                            .append("\tAP\t")
                            .append(pointFlag)
                            .append("\t")
                            .append(point.getId());
                    output.add(record.toString());
                }

                if ((pointFlag.equals("C") || pointFlag.equals("B")) && point.getPType() == PType.OM) {
                    record.setLength(0);
                    record.append(partitionId)
                            .append("\t")
                            .append(point.getClusterId())
                            .append("\tBP\t")
                            .append(pointFlag)
                            .append("\t")
                            .append(point.getId());
                    output.add(record.toString());
                }
            }

            return output.iterator();
        });

        results.saveAsTextFile("/job/DBSCAN/output/" + partitionFile + "_local");

        jsc.close();
        spark.stop();
    }
}