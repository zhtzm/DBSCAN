package com.java;

import com.java.mapper.Hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

import static com.java.MR_DBSCAN.hive;

public class Merging {
    public static final String Data = "data";

    public static void merging(String tableName, int numField) throws SQLException, ClassNotFoundException {
        createTable(tableName);

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始获取分区对");
        List<Integer> ps = new ArrayList<>();
        try (ResultSet res = hive.sql(null, "select distinct pid from local_tmp")) {
            while (res.next()) {
                ps.add(Integer.parseInt(res.getString(1)));
            }
        }
        Hive.destroy();
        List<List<Integer>> pairs = generateUnorderedPairs(ps);
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "获取分区对完成");

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始局部映射");
        for (List<Integer> pair : pairs) {
            buildMappingMAP(pair);
        }
        filterTable();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "局部映射完成");

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始全局映射");
        globalMapping();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "全局映射完成");

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始最终合并");
        finalMerge(tableName, numField);
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "最终合并完成");

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始删除临时表");
        dropTmpTable();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "删除临时表完成");
    }

    private static void finalMerge(String tableName, int numField) throws SQLException, ClassNotFoundException {
        StringBuilder fieldStr = new StringBuilder();
        for(int ddl = 0; ddl < numField; ++ddl) {
            fieldStr.append("d.x").append(ddl);
            if(ddl != numField - 1) {
                fieldStr.append(", ");
            }
        }

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始建表:final_merge");
        if(hive.existsTable(null, "final_merge")) {
            hive.dropTable(null, "final_merge");
            Hive.destroy();
        }
        String var4 = "CREATE TABLE final_merge AS " +
                "SELECT mf.cid2, lt.ct, " + fieldStr + " " +
                "FROM local_tmp lt " +
                "JOIN map_final mf ON lt.cid = mf.cid1 " +
                "JOIN " + tableName + " d ON lt.id = d.id " +
                "WHERE lt.pt='FP'";
        hive.ddl(null, var4);
        Hive.destroy();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "建表完成:final_merge");
    }

    private static void buildMappingMAP(List<Integer> pair) throws SQLException, ClassNotFoundException {
        int i = pair.get(0);
        int j = pair.get(1);
        String insertSqlIJ = "INSERT INTO merge_tmp (cid1, cid2) " +
                "SELECT t1.cid AS cid1, t2.cid AS cid2 FROM " +
                "(SELECT cid, id FROM local_tmp WHERE pid=" + i + " AND pt='AP') t1 " +
                "INNER JOIN " +
                "(SELECT cid, id FROM local_tmp WHERE pid=" + j + " AND pt='BP') t2 " +
                "ON t1.id = t2.id";
        String insertSqlJI = "INSERT INTO merge_tmp (cid1, cid2) " +
                "SELECT t1.cid AS cid1, t2.cid AS cid2 FROM " +
                "(SELECT cid, id FROM local_tmp WHERE pid=" + j + " AND pt='AP') t1 " +
                "INNER JOIN " +
                "(SELECT cid, id FROM local_tmp WHERE pid=" + i + " AND pt='BP') t2 " +
                "ON t1.id = t2.id";
        hive.update(null, insertSqlIJ);
        Hive.destroy();
        hive.update(null, insertSqlJI);
        Hive.destroy();
    }

    private static void filterTable() throws SQLException, ClassNotFoundException {
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始建表:merge_filter");
        if(hive.existsTable(null, "merge_filter")) {
            hive.dropTable(null, "merge_filter");
            Hive.destroy();
        }
        String ddl = "CREATE TABLE merge_filter " +
                "AS SELECT DISTINCT LEAST(cid1, cid2) AS min_cid, GREATEST(cid1, cid2) AS max_cid " +
                "FROM merge_tmp";
        hive.ddl(null, ddl);
        Hive.destroy();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "建表完成:merge_filter");
    }

    private static void globalMapping() throws SQLException, ClassNotFoundException {
        List<List<Integer>> mapL = new ArrayList<>();
        try (ResultSet res = hive.sql(null, "select min_cid, max_cid from merge_filter")) {
            while (res.next()) {
                mapL.add(
                        Arrays.asList(Integer.parseInt(res.getString(1)),
                                Integer.parseInt(res.getString(2)))
                );
            }
        }
        Hive.destroy();

        List<Integer> clusters = new ArrayList<>();
        try (ResultSet res = hive.sql(null, "SELECT DISTINCT cid from local_tmp")) {
            while (res.next()) {
                clusters.add(Integer.parseInt(res.getString(1)));
            }
        }
        Hive.destroy();

        Map<Integer, Integer> clusterMap = getIntegerIntegerMap(mapL);
        Map<Integer, Integer> allMap = new HashMap<>();
        for (Integer cid : clusters) {
            int v = clusterMap.getOrDefault(cid, cid);
            allMap.put(cid, v);
        }

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始建表:map_final");
        if(hive.existsTable(null, "map_final")) {
            hive.dropTable(null, "map_final");
            Hive.destroy();
        }
        hive.ddl(null, "create table if not exists map_final (cid1 int, cid2 int) ");
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "建表完成:map_final");
        Hive.destroy();

        for (Map.Entry<Integer, Integer> entry : allMap.entrySet()) {
            int originalCid = entry.getKey();
            int newCid = entry.getValue();
            String insert = "INSERT INTO map_final (cid1, cid2) VALUES (" + originalCid + "," + newCid + ")";
            hive.update(null, insert);
        }
        Hive.destroy();
    }

    private static Map<Integer, Integer> getIntegerIntegerMap(List<List<Integer>> mapL) {
        Map<Integer, Integer> clusterMap = new HashMap<>();
        for(List<Integer> pair : mapL) {
            if(pair.size() != 2) {
                throw new IllegalArgumentException("Cluster list must contain exactly two elements.");
            }

            int cid1 = pair.get(0);
            int cid2 = pair.get(1);
            if(clusterMap.containsKey(cid1)) {
                cid1 = clusterMap.get(cid1);
            } else if(clusterMap.containsKey(cid2)) {
                int tmp_cid = cid1;
                cid1 = clusterMap.get(cid2);
                cid2 = tmp_cid;
            }

            clusterMap.put(cid2, cid1);
        }

        return clusterMap;
    }

    private static List<List<Integer>> generateUnorderedPairs(List<Integer> clusters) {
        List<List<Integer>> pairs = new ArrayList<>();
        Collections.sort(clusters);

        for(int i = 0; i < clusters.size(); ++i) {
            for(int j = i + 1; j < clusters.size(); ++j) {
                pairs.add(Arrays.asList(clusters.get(i), clusters.get(j)));
            }
        }

        return pairs;
    }

    private static void createTable(String tableName) throws ClassNotFoundException, SQLException {
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始建表:local_tmp");
        if(hive.existsTable(null, "local_tmp")) {
            hive.dropTable(null, "local_tmp");
            Hive.destroy();
        }
        String ddlL = "create table if not exists local_tmp " +
                "(pid INT, cid INT, pt STRING, ct STRING, id INT) " +
                "row format delimited fields terminated by \t' " +
                "lines terminated by \n";
        hive.ddl(null, ddlL);
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "建表完成:local_tmp");
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "导入数据:local_tmp");
        hive.loadData(null, "local_tmp", "/job/DBSCAN/output/" + tableName + "_local");
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "导入完成:local_tmp");
        Hive.destroy();

        if(hive.existsTable(null, "merge_tmp")) {
            hive.dropTable(null, "merge_tmp");
            Hive.destroy();
        }
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始建表:merge_tmp");
        String ddlM = "create table if not exists merge_tmp (cid1 int, cid2 int) ";
        hive.ddl(null, ddlM);
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "建表完成:merge_tmp");
        Hive.destroy();
    }

    private static void dropTmpTable() throws SQLException, ClassNotFoundException {
        if(hive.existsTable(null, "local_tmp")) {
            hive.dropTable(null, "local_tmp");
            Hive.destroy();
        }

        if(hive.existsTable(null, "merge_tmp")) {
            hive.dropTable(null, "merge_tmp");
            Hive.destroy();
        }

        if(hive.existsTable(null, "merge_filter")) {
            hive.dropTable(null, "merge_filter");
            Hive.destroy();
        }

        if(hive.existsTable(null, "map_final")) {
            hive.dropTable(null, "map_final");
            Hive.destroy();
        }
    }
}
