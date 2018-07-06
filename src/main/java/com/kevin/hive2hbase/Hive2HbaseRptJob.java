package com.kevin.hive2hbase;

import com.kevin.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Kevin
 * @date 18-7-6
 * @Description：
 */
public class Hive2HbaseRptJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException {

        String hivetablename = "rpt_job";
        String hbasetablename = "rpt_job";
        String hiveDbname = "jobs";
        String date = ""; // 从控制台读入日期，格式为yyyymmdd
        if (args.length == 0) {
            System.out.println("input date empty!");
            return;
        }
        date = "20180702";
        // 判断Hive分区文件是否存在
        String hivepath = "/user/hive/warehouse/jobs.db/" + hivetablename + "/pt=" + date;
        if (HDFSUtil.isDirExists(hivepath)) {
            Hive2HbaseRptJob.hive2Hbase(hiveDbname, hivetablename, hbasetablename, date);
            System.out.println("Insert finished!");
        } else {
            System.out.println("【hive path doesn't exist!】" + hivepath);
        }
    }

    public static boolean hive2Hbase(String hiveDbName, String hiveTableName, String hbaseTableName, String dayPartition) throws IOException, InterruptedException, ClassNotFoundException, SQLException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("dfs.permissions", "false");
        // 从本地加载
        String hadoop_home = System.getenv("HADOOP_HOME");
        String hhase_home = System.getenv("HBASE_HOME");
        conf.addResource(new Path(hadoop_home + "/conf/mapred-site.xml"));
        conf.addResource(new Path(hadoop_home + "/conf/core-site.xml"));
        conf.addResource(new Path(hhase_home + "/conf/hbase-site.xml"));
        conf.addResource(new Path(hadoop_home + "/conf/yarn-site.xml"));

        conf.set("hbasetable", hbaseTableName);
        conf.set("job_date", dayPartition);
        // 把Hive中的所有列都导入HBase表
        String hivetablecolumnlist = "";
        // 把所有列表存储在字符串中，以逗号分隔
        //List<String> colNameLst = HiveJdbcUtil.getTableColumn(PropertiesUtil.getPropertyValue("hive.jdbcurl"), hiveDbName + "." + hiveTableName);
        List<String> colNameLst = null;
        // 获取列名
        for (int i = 0; i < colNameLst.size(); i++) {
            if (i == colNameLst.size() - 1) {
                hivetablecolumnlist = hivetablecolumnlist + colNameLst.get(i);
            } else {
                hivetablecolumnlist = hivetablecolumnlist + colNameLst.get(i) + ",";
            }
        }
        conf.set("hivetablecolumnlist", hivetablecolumnlist);
        Job job = Job.getInstance(conf, "Hive2Hbase rpt_job ");
        job.setJarByClass(Hive2HbaseRptJob.class);
        job.setMapperClass(MapFromHiveSeqfile.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        Path in = new Path("/user/hive/warehouse/jobs.db/" + hiveTableName + "/pt=" + dayPartition);
        SequenceFileAsTextInputFormat.addInputPath(job, in);
        job.setReducerClass(ReduceToHbase.class);
        TableMapReduceUtil.initTableReducerJob(hbaseTableName, ReduceToHbase.class, job);
        return job.waitForCompletion(true);
    }
}
