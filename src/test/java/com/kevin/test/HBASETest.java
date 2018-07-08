package com.kevin.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HBASETest {
    public static Configuration configuration;
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("dfs.permissions", "false");
        // 从本地加载
        String hadoop_home = "/soft/hadoop";
        String hhase_home = "/soft/hbase";
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/mapred-site.xml"));
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(hhase_home + "/conf/hbase-site.xml"));
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/yarn-site.xml"));
    }
    public static void main(String[] args) {

    }

    public static void insertData(String tableName) {
        System.out.println("start insert data ......");
        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
        put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
        put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");
    }
}
