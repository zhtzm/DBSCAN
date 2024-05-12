package com.java.mapper;

import java.sql.*;

@SuppressWarnings("SqlNoDataSourceInspection")
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

    @SuppressWarnings("SqlNoDataSourceInspection")
    public void showDBS() throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        res = state.executeQuery("show databases");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public void createDB(String newDBName) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        state.execute(String.format("create database %s", newDBName));
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public void dropDB(String DBName) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        state = con.createStatement();
        state.execute(String.format("drop database if exists %s CASCADE", DBName));
    }

    public static void init(String DBName) throws SQLException, ClassNotFoundException {
        destroy();
        Class.forName(DRIVE);
        con = DriverManager.getConnection(URL + "/" + DBName, USERNAME, PASSWORD);
        state = con.createStatement();
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
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
        //字段与字段之间的分隔符
        String sql = "create table if not exists " +
                tableName +
                " (" + fieldStr + ")" +
                "row format delimited " +
                "fields terminated by '" + terminated + "' " +
                "lines terminated by '\n' ";
        state.execute(sql);
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
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

    public Boolean existsTable(String DBName, String tableName) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        String sql = "SHOW TABLES LIKE '" + tableName + "'";
        res = state.executeQuery(sql);
        return res.next();
    }

    public void dropTable(String DBName, String tableName) throws SQLException, ClassNotFoundException {
        if(DBName == null) {
            init("job");
        } else {
            init(DBName);
        }

        String sql = "DROP TABLE " + tableName;
        state.execute(sql);
    }

    public void loadData(String DBName, String tableName, String path) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        state.execute("load data inpath '" + path  + "' overwrite into table " + tableName);
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

    public void ddl(String DBName, String ddl) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        state.execute(ddl);
    }

    public void update(String DBName, String sql) throws SQLException, ClassNotFoundException {
        if (DBName == null) {
            init(DBNAME);
        } else {
            init(DBName);
        }
        state.executeUpdate(sql);
    }
}
