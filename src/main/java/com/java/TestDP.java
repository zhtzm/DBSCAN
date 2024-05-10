package com.java;

import java.io.IOException;
import java.sql.SQLException;

public class TestDP {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        MR_DBSCAN.uploadLocalFile("C:/Users/zhouh/Downloads/farm.ds", 5);
        new DataPartitioning(MR_DBSCAN.tableName, 5, 500).partitioning(500000);
    }
}
