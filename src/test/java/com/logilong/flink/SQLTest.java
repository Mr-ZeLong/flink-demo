package com.logilong.flink;

import jakarta.annotation.Resource;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SQLTest {
    @Resource
    private StreamTableEnvironment tableEnv;
    @Test
    // 需求1：实时统计直播间在线人数（5 分钟滚动窗口）
    public void test1() throws Exception{
        tableEnv.executeSql("CREATE TABLE live_user_behavior (\n" +
                "    user_id STRING,\n" +
                "    room_id STRING,\n" +
                "    behavior_type STRING, -- 'view', 'like', 'share', 'comment', 'click_product'\n" +
                "    product_id STRING,\n" +
                "    behavior_time BIGINT, -- 事件时间戳(毫秒)\n" +
                "    proc_time AS PROCTIME(), -- 处理时间\n" +
                "    event_time AS TO_TIMESTAMP_LTZ(behavior_time, 3), -- 事件时间\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND -- 水印，允许5秒延迟\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'live_user_behavior',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'flink_sql_live_analysis',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ");").print();

        tableEnv.sqlQuery("select * from live_user_behavior").execute().print();

    }
}
