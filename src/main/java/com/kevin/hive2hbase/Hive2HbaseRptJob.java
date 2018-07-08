package com.kevin.hive2hbase;

import com.kevin.util.HDFSUtil;
import com.kevin.util.HIVEUtil;
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
        String date = "20180702"; // 从控制台读入日期，格式为yyyymmdd
        // 判断Hive分区文件是否存在
        String hivepath = "/hive/warehouse/jobs.db/" + hivetablename + "/pt=" + date;
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
        String hadoop_home = "/soft/hadoop";
        String hhase_home = "/soft/hbase";
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/mapred-site.xml"));
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(hhase_home + "/conf/hbase-site.xml"));
        conf.addResource(new Path(hadoop_home + "/etc/hadoop/yarn-site.xml"));

        conf.set("hbasetable", hbaseTableName);
        conf.set("job_date", dayPartition);
        // 把Hive中的所有列都导入HBase表
        String hivetablecolumnlist = HIVEUtil.getTableColumn(hiveTableName);
        conf.set("hivetablecolumnlist", hivetablecolumnlist);
        Job job = Job.getInstance(conf, "Hive2Hbase rpt_job ");
        job.setJarByClass(Hive2HbaseRptJob.class);
        job.setMapperClass(MapFromHiveSeqfile.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        Path in = new Path("/hive/warehouse/jobs.db/" + hiveTableName + "/pt=" + dayPartition);
        SequenceFileAsTextInputFormat.addInputPath(job, in);
        job.setReducerClass(ReduceToHbase.class);
        TableMapReduceUtil.initTableReducerJob(hbaseTableName, ReduceToHbase.class, job);
        return job.waitForCompletion(true);
    }
}
