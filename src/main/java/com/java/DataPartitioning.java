package com.java;

import com.java.mapper.HDFS;
import com.java.mapper.Hive;
import com.java.ds.TreeNode;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitioning {
    private static final HDFS hdfs = new HDFS();
    private static final Hive hive = new Hive();
    private static final Map<Integer, TreeNode> S_map = new HashMap<>();
    private final String table_name;
    private final int num_field;
    private final double threshold;

    public DataPartitioning(String table_name, int num_field, double threshold) {
        this.table_name = table_name;
        this.num_field = num_field;
        this.threshold = threshold;
    }

    public void partitioning(int partitionNum)
            throws SQLException, ClassNotFoundException, IOException {
        Double[] minArray = new Double[num_field];
        Double[] maxArray = new Double[num_field];

        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "获取各维度上下界");
        for (int i = 0; i < num_field; i++) {
            try (ResultSet res = hive.sql(null,
                    "SELECT MIN(x" + i + ") AS min_value FROM " + table_name)) {
                if (res.next()) {
                    minArray[i] = Double.parseDouble(res.getString("min_value"));
                }
            }
            Hive.destroy();
            try (ResultSet res = hive.sql(null,
                    "SELECT MAX(x" + i + ") AS max_value FROM " + table_name)) {
                if (res.next()) {
                    maxArray[i] = Double.parseDouble(res.getString("max_value")) + 1.0;
                }
            }
        }
        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "获取成功");

        long num = 0;
        try (ResultSet res = hive.sql(null,
                "SELECT COUNT(*) FROM " + table_name)) {
            if (res.next()) {
                num = Long.parseLong(res.getString(1));
            }
        }

        TreeNode root = new TreeNode(-1, minArray, maxArray, num);
        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "开始建树");
        buildTree(root, 0, partitionNum);
        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "建树完成");

        List<TreeNode> leafNodes = new ArrayList<>();
        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "获取叶子节点");
        getLeafNodes(leafNodes, root);

        int index = 0; // 从1开始计数
        for (TreeNode leaf : leafNodes) {
            S_map.put(index++, leaf); // 将叶子节点添加到map中，并递增index
        }

        System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + "分区导入HDFS");
        hdfsSP();
    }

    private void buildTree(TreeNode parent, int split, long max_num) throws SQLException, ClassNotFoundException {
        Double[] bottom = parent.getBottom();
        Double[] top = parent.getTop();
        double split_val = (top[split] + bottom[split]) / 2;
        Double[] new_left_top = new Double[num_field];
        Double[] new_right_bottom = new Double[num_field];
        int new_split = (split + 1) % num_field;

        for (int i = 0; i < num_field; i++) {
            if (i == split) {
                new_left_top[i] = split_val;
                new_right_bottom[i] = split_val;
            } else {
                new_left_top[i] = top[i];
                new_right_bottom[i] = bottom[i];
            }
        }

        long left_count = count(bottom, new_left_top);
        long right_count = count(new_right_bottom, top);

        TreeNode left = new TreeNode(split, bottom, new_left_top, left_count);
        parent.setLeft(left);
        if (left_count > max_num)
            buildTree(parent.getLeft(), new_split, max_num);

        TreeNode right = new TreeNode(split, new_right_bottom, top, right_count);
        parent.setRight(right);
        if (right_count > max_num)
            buildTree(parent.getRight(), new_split, max_num);
    }

    private long count(Double[] min, Double[] max)
            throws SQLException, ClassNotFoundException {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM " + table_name + " WHERE ");

        for (int i = 0; i < num_field; i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append("x").append(i).append(" >= ").append(min[i])
                    .append(" AND x").append(i).append(" < ").append(max[i]);
        }

        long num = 0;
        try (ResultSet res = hive.sql(null, sql.toString())) {
            if (res.next()) {
                num = Long.parseLong(res.getString(1));
            }
        }

        return num;
    }

    private void getLeafNodes(List<TreeNode> leafNodes, TreeNode root) {
        if (root == null) {
            return;
        }

        if (root.getLeft() == null && root.getRight() == null) {
            leafNodes.add(root);
        }

        getLeafNodes(leafNodes, root.getLeft());
        getLeafNodes(leafNodes, root.getRight());
    }

    private void hdfsSP() throws SQLException, ClassNotFoundException, IOException {
        String outputPath = HDFS.JOB_PATH + "/output";
        Path file = new Path(outputPath + "/" + "partition.txt");
        hdfs.init();
        FSDataOutputStream outputStream = hdfs.createFile(file);

        for (int i = 0; i < S_map.size(); i++) {
            Double[] min = S_map.get(i).getBottom();
            Double[] max = S_map.get(i).getTop();

            System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + i +"分区IR导入HDFS");
            StringBuilder sqlIR = new StringBuilder("SELECT * FROM " + table_name + " WHERE ");
            for (int j = 0; j < num_field; j++) {
                if (j > 0) {
                    sqlIR.append(" AND ");
                }
                sqlIR.append("x").append(j).append(" >= ").append(min[j] + threshold)
                        .append(" AND x").append(j).append(" < ").append(max[j] - threshold);
            }
            try (ResultSet res1 = hive.sql(null, sqlIR.toString())) {
                while (res1.next()) {
                    StringBuilder value = new StringBuilder(i + "\t" + "IR ");
                    for (int k = 2; k < num_field + 1; k++) {
                        value.append(res1.getString(k)).append(" ");
                    }
                    value.append(res1.getString(num_field + 1)).append("\t");
                    value.append(res1.getString(1)).append("\n");
                    outputStream.writeBytes(value.toString());
                }
            }
            Hive.destroy();

            System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + i +"分区IM导入HDFS");
            StringBuilder sqlIM = new StringBuilder("SELECT * FROM " + table_name + " WHERE (");
            for (int j = 0; j < num_field; j++) {
                if (j > 0) {
                    sqlIM.append(" AND ");
                }
                sqlIM.append("x").append(j).append(" >= ").append(min[j])
                        .append(" AND x").append(j).append(" < ").append(max[j]);
            }
            sqlIM.append(") AND NOT (");
            for (int j = 0; j < num_field; j++) {
                if (j > 0) {
                    sqlIM.append(" AND ");
                }
                sqlIM.append("x").append(j).append(" >= ").append(min[j] + threshold)
                        .append(" AND x").append(j).append(" < ").append(max[j] - threshold);
            }
            sqlIM.append(")");
            try (ResultSet res2 = hive.sql(null, sqlIM.toString())) {
                while (res2.next()) {
                    StringBuilder value = new StringBuilder(i + "\t" + "IM ");
                    for (int k = 2; k < num_field + 1; k++) {
                        value.append(res2.getString(k)).append(" ");
                    }
                    value.append(res2.getString(num_field + 1)).append("\t");
                    value.append(res2.getString(1)).append("\n");
                    outputStream.writeBytes(value.toString());
                }
            }
            Hive.destroy();

            System.out.println(MR_DBSCAN.formatter.format(LocalDateTime.now()) + i +"分区OM导入HDFS");
            StringBuilder sqlOM = new StringBuilder("SELECT * FROM " + table_name + " WHERE (");
            for (int j = 0; j < num_field; j++) {
                if (j > 0) {
                    sqlOM.append(" AND ");
                }
                sqlOM.append("x").append(j).append(" >= ").append(min[j] - threshold)
                        .append(" AND x").append(j).append(" < ").append(max[j] + threshold);
            }
            sqlOM.append(") AND NOT (");
            for (int j = 0; j < num_field; j++) {
                if (j > 0) {
                    sqlOM.append(" AND ");
                }
                sqlOM.append("x").append(j).append(" >= ").append(min[j])
                        .append(" AND x").append(j).append(" < ").append(max[j]);
            }
            sqlOM.append(")");
            try (ResultSet res3 = hive.sql(null, sqlOM.toString())) {
                while (res3.next()) {
                    StringBuilder value = new StringBuilder(i + "\t" + "OM ");
                    for (int k = 2; k < num_field + 1; k++) {
                        value.append(res3.getString(k)).append(" ");
                    }
                    value.append(res3.getString(num_field + 1)).append("\t");
                    value.append(res3.getString(1)).append("\n");
                    outputStream.writeBytes(value.toString());
                }
            }
            Hive.destroy();
        }

        hdfs.close();
        System.out.println("Data written to HDFS successfully.");
    }
}
