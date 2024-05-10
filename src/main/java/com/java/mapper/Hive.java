package com.java.mapper;

import java.sql.*;

public class Hive {
    private static final String DRIVE = "org.apache.hive.jdbc.HiveDriver";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String URL = "jdbc:hive2://centos01:10000";
    private static final String DBNAME = "job";
    private static Connection con = null;
    private static Statement state = null;
    private static ResultSet res = null;

    public static void destroy() throws SQLException {
        if (res != null) state.close();
        if (state != null) state.close();
        if (con != null) con.close();
    }

    public void showDBS() throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        res = state.executeQuery("show databases");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    public void createDB(String newDBName) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        state.execute("create database " + newDBName);
    }

    public void dropDB(String DBName) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        state.execute("drop database if exists " + DBName + " CASCADE");
    }

    public static void init(String DBName) throws SQLException, ClassNotFoundException {
        destroy();
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL + "/" + DBName, USERNAME, PASSWORD);
        state = con.createStatement();
    }

    public void createTable(String DBName, String tableName, Integer num_field, String terminated)
            throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        StringBuilder fieldStr = new StringBuilder();
        fieldStr.append("id int,");
        for (int i = 0; i < num_field; i++) {
            fieldStr.append("x").append(i).append(" DOUBLE");
            if (i != num_field - 1)
                fieldStr.append(",");
        }
        String sql = "create table if not exists " + tableName + " (" +
                fieldStr + ")" +
                "row format delimited " +
                "fields terminated by '" + terminated + "' " +//字段与字段之间的分隔符
                "lines terminated by '\n' ";
        state.execute(sql);
    }

    public void showTables(String DBName) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        res = state.executeQuery("show tables");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    public void descTable(String DBName, String tableName) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        res = state.executeQuery("desc student");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    public void loadData(String DBName, String tableName, String path) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        state.execute("load data inpath '" + path  + "' overwrite into table " + tableName);
    }

    public void countData(String DBName, String tableName) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        res = state.executeQuery("select count(1) from " + tableName);
        while (res.next()) {
            System.out.println(res.getInt(1));
        }
    }

    public ResultSet sql(String DBName, String sql) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        res = state.executeQuery(sql);
        return res;
    }
}
