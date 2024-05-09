package com.java;

import java.io.IOException;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        new DataPartitioning("onewdata", 2, 20000.).partitioning();
    }
}
