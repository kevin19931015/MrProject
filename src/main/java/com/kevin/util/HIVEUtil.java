package com.kevin.util;

import java.sql.*;

/**
 * @author Kevin
 * @date 18-7-6
 * @Description：
 */
public class HIVEUtil {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";//jdbc驱动路径
    private static String url = "jdbc:hive2://localhost:10000/jobs";//hive库地址+库名
    private static String user = "root";//用户名
    private static String password = "123456";//密码
    private static String sql;
    private static ResultSet res;
    private static Statement stmt;
    private static Connection conn;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ResultSet exec(String sql) {
        System.out.println("Running: " + sql);
        try {
            res = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }

    public static String getTableColumn(String tableName) throws SQLException {
        sql = "select * from "+tableName+" limit 1";
        ResultSet res = exec(sql);
        String tableCloumns = "";
        while (res.next()){
            ResultSetMetaData resMetaData = res.getMetaData();
            tableCloumns = resMetaData.getColumnName(1)+","+resMetaData.getColumnName(2)+","+
                    resMetaData.getColumnName(3)+","+resMetaData.getColumnName(4)+","+
                    resMetaData.getColumnName(5)+","+resMetaData.getColumnName(6)+","+
                    resMetaData.getColumnName(7)+","+resMetaData.getColumnName(8)+","+
                    resMetaData.getColumnName(9)+","+resMetaData.getColumnName(10)+","+
                    resMetaData.getColumnName(11)+","+resMetaData.getColumnName(12)+","+
                    resMetaData.getColumnName(13)+","+resMetaData.getColumnName(14)+","+
                    resMetaData.getColumnName(15)+","+resMetaData.getColumnName(16)+","+
                    resMetaData.getColumnName(17)+","+resMetaData.getColumnName(18)+","+
                    resMetaData.getColumnName(19);
        }
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) { /* ignored */}
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) { /* ignored */}
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) { /* ignored */}
        }
        return tableCloumns;
    }

}
