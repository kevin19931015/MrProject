package com.kevin.test;

import java.sql.*;

/**
 * @author Kevin
 * @date 18-7-6
 * @Description：
 */
public class HIVETest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";//jdbc驱动路径
    private static String url = "jdbc:hive2://localhost:10000/jobs";//hive库地址+库名
    private static String user = "kevin";//用户名
    private static String password = "123456";//密码
    private static String sql = "SELECT * FROM rpt_job limit 1";
    private static ResultSet res;

    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行 select * query 运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getInt(2));
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}
