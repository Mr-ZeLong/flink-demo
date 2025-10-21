package com.logilong.flink;

import com.logilong.flink.producer.KafkaProducer;
import jakarta.annotation.Resource;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Random;

@SpringBootTest
public class SQLTest {
    @Resource
    private StreamTableEnvironment tableEnv;
    @Resource
    private KafkaProducer kafkaProducer;

    @BeforeEach
    public void before() throws Exception {
        // 用户行为流表 (Kafka)
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
                "    'properties.bootstrap.servers' = '172.28.254.108:9092',\n" +
                "    'properties.group.id' = 'flink_sql_live_analysis',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'latest-offset'\n" +
                ");").print();
    }


    public void mockData(){
        // 模拟数据
        new Thread(() -> {
            while (true) {
                try {
                    // 构造随机数据
                    Random random = new Random();
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("user_id", "u" + (1000 + random.nextInt(10)));
                    jsonObject.put("room_id", "r" + (2000 + random.nextInt(10)));
                    String[] behaviorTypes = {"view", "like", "share", "comment", "click_product"};
                    jsonObject.put("behavior_type", behaviorTypes[(int) (Math.random() * behaviorTypes.length)]);
                    if ("click_product".equals(jsonObject.getString("behavior_type"))) {
                        jsonObject.put("product_id", "p" + (3000 + random.nextInt(10)));
                    } else {
                        jsonObject.put("product_id", JSONObject.NULL);
                    }

                    jsonObject.put("behavior_time", System.currentTimeMillis() + 1000 - random.nextInt(2000));
                    // 发送数据到Kafka
                    kafkaProducer.sendMessage("live_user_behavior", jsonObject.toString());
                    // 每隔0.5秒发送一条数据
                    Thread.sleep(500);
                } catch (InterruptedException | JSONException e) {

                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }

    @Test
    // 需求1：实时统计直播间在线人数（5 分钟滚动窗口）
    public void test1() throws Exception{
        mockData();
        tableEnv.executeSql("" +
                "select\n" +
                "     room_id\n" +
                "    ,window_start\n" +
                "    ,window_end\n" +
                "    ,count(*) as pv\n" +
                "    ,count(distinct user_id) as uv\n" +
                "    ,sum(case when behavior_type = 'view' then 1 else 0 end) as view_count\n" +
                "    ,sum(case when behavior_type = 'like' then 1 else 0 end) as like_count\n" +
                "    ,sum(case when behavior_type = 'share' then 1 else 0 end) as share_count\n" +
                "    ,sum(case when behavior_type = 'comment' then 1 else 0 end) as comment_count\n" +
                "    ,sum(case when behavior_type = 'click_product' then 1 else 0 end) as click_product_count\n" +
                "from table(tumble(table live_user_behavior, descriptor(event_time), interval '5' minute, ALLOWED_LATENESS => INTERVAL '2' MINUTE))\n" +
                "group by\n" +
                "     room_id\n" +
                "    ,window_start\n" +
                "    ,window_end\n" +
                ";").print();

    }

    @Resource private RedisTemplate<String, Long> redisTemplate;
    @Test
    public void test2(){
        mockData();
        tableEnv.sqlQuery("" +
                "SELECT\n" +
                "    room_id\n" +
                "    ,window_start\n" +
                "    ,window_end\n" +
                "    ,COUNT(DISTINCT user_id) AS online_count\n" +
                "FROM table(tumble(table live_user_behavior, descriptor(event_time), interval '1' minute))\n" +
                "WHERE behavior_type = 'view'\n" +
                "-- 最近1分钟内有观看行为的用户视为在线\n" +
                "GROUP BY \n" +
                "    room_id\n" +
                "    ,window_start\n" +
                "    ,window_end").execute().collect().forEachRemaining(row -> {
                    redisTemplate.opsForValue().set(row.getFieldAs(0) + "_" + "online_cnt", row.getFieldAs(3));
        });
    }


}
