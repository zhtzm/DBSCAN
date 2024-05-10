package com.java.MR;

import com.java.DBSCAN;
import com.java.ds.CType;
import com.java.ds.PType;
import com.java.ds.Point;
import com.java.mapper.HDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LocalClustering {
    public static int MinPts;
    public static double Epsilon;

    public static void clustering(int minPts, double epsilon, String partitionFile)
            throws IOException, InterruptedException, ClassNotFoundException {
        MinPts = minPts;
        Epsilon = epsilon;

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Local Clustering");
        job.setJarByClass(LocalClustering.class);

        job.setMapperClass(LocalClustering.PartitionClusteringMapper.class);
        job.setReducerClass(LocalClustering.PartitionClusteringReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(HDFS.JOB_PATH + "/output/" + partitionFile));
        FileOutputFormat.setOutputPath(job, new Path(HDFS.JOB_PATH + "/output/local"));

        job.waitForCompletion(true);
    }

    public static class PartitionClusteringMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t"); // 输入是 "分区id\t坐标" 格式，用空格分隔
            if (parts.length == 3) {
                context.write(new Text(parts[0]), new Text( parts[2] + " " + parts[1]));
            }
        }
    }

    public static class PartitionClusteringReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            List<Point> pointList = new ArrayList<>();

            for (Text text : values) {
                String line = text.toString();
                Point p = new Point(line, Integer.parseInt(key.toString()));
                p.setPartitionId(Integer.parseInt(key.toString()));
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

                // key:PartitionId record:ClusterId (AP, BP, FP) (C, B, N) [x1,x2,x3,...]
                context.write(key, new Text("FP\t" + pointFlag + "\t" + point.getId()));
                if (pointFlag.equals("C") && point.getPType() == PType.IM) {
                    context.write(key, new Text("AP\t" + pointFlag + "\t" + point.getId()));
                }
                if ((pointFlag.equals("C") || pointFlag.equals("B")) && point.getPType() == PType.OM) {
                    context.write(key, new Text("BP\t" + pointFlag + "\t" + point.getId()));
                }
            }
        }
    }
}
