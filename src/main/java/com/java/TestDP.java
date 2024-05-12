package com.java;

import java.io.IOException;
import java.sql.SQLException;

public class TestDP {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        MR_DBSCAN.uploadLocalFile("C:/Users/zhouh/Downloads/10000data.txt", 2);
        new DataPartitioning(MR_DBSCAN.TableName, 2, 20000).partitioning(5000);
    }
}
