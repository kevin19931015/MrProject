package com.kevin.test;

import com.kevin.util.HDFSUtil;

public class HDFSTest {
    public static void main(String[] args) {
        boolean exist = HDFSUtil.isDirExists("/user/hive/warehouse/jobs.db/rpt_job/pt=20180702");
        System.out.println(exist);
    }
}