package com.kevin.hive2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kevin
 * @date 18-7-6
 * @Description：
 */
public class ReduceToHbase extends TableReducer<Text, Text, Text> {
    private List<String> hiveColumnLst = new ArrayList<String>();
    private String hbasetablename = "";
    private String family = "cf";
    // 职位发布日期，pt的内容
    private String job_date = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        hbasetablename = conf.get("hbasetable");
        job_date = conf.get("job_date");
        String hivetablecolumnlist = conf.get("hivetablecolumnlist");
        for (String x : hivetablecolumnlist.split(",")) {
            hiveColumnLst.add(x);
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] valArray = val.toString().split(String.valueOf(","));
            // valArray中不包含pt分区列，而hiveColumnLst中包含pt分区列，两者相差1
            if (valArray.length == hiveColumnLst.size() - 1) {
                byte[] b_rowkey = Bytes.toBytes(key.toString());
                byte[] b_family = Bytes.toBytes(family);
                for (int x = 0; x < valArray.length; x++) {
                    Put put = new Put(b_rowkey); // rowkey
                    String each_val = valArray[x];
                    byte[] b_qualifier = Bytes.toBytes(hiveColumnLst.get(x));
                    byte[] b_val = Bytes.toBytes(each_val);
                    put.addColumn(b_family,b_qualifier,b_val);
                    context.write(new Text(hbasetablename), put);
                }
            }
        }
    }
}
