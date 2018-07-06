package com.kevin.hive2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Kevin
 * @date 18-7-6
 * @Description：
 */
public class MapFromHiveSeqfile extends Mapper<Text, Text, Text, Text> {
    private String job_date = "";

    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        job_date = conf.get("job_date");
    }

    public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
        String[] valueArray = values.toString().split(",");


        String rowkey = "";
        if (valueArray.length == 25) {
            rowkey += job_date + "_";  // 职位发布日期
            rowkey += valueArray[1];  // 网站来源
            rowkey += valueArray[5];  // 工作地点
            rowkey += valueArray[7];  // 学历要求
            rowkey += valueArray[12];  // 工作年限
            rowkey += valueArray[14];  // 薪资
            rowkey += "_" + valueArray[3];  // 职位名称
            rowkey += "_" + valueArray[16];  // 公司名称
        }
        // 把整行内容直接放入value中
        context.write(new Text(rowkey), values);
    }
}