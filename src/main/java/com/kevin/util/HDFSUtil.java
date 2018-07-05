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
     * @params：[filePath]
     * @return：boolean
     * @throws
     * @Description：
     * @author Kevin
     * @date 18-7-5
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
