package com.kevin.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSUtil {
    private static Configuration conf = new Configuration();

    static {
        //conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
    }

    /**
     * @Title: isDirExists
     * @param：@param filePath 文件（文件夹）路径
     * @return：boolean 存在返回true，不存在返回false
     * @Description：TODO 判断文件或文件夹是否存在
     * @author Kevin
     * @date 2018年7月05日 下午4:13:34
     * @throws
     */
    public static boolean isDirExists(String filePath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path fileName = new Path(filePath);
            return fs.exists(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }
}
