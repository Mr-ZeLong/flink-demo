package com.logilong.flink;

import com.logilong.flink.producer.KafkaProducer;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Slf4j
class FlinkDemoApplicationTests {
    @Resource
    private StreamExecutionEnvironment env;
    @Test
    void wordCountTest() throws Exception {
        env.fromData("hello world")
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                 .keyBy((KeySelector<Tuple2<String, Integer>, String>) kv -> kv.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

         env.execute();
    }

    @Test
    public void fileSourceTest() throws Exception {
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\Code\\flink-demo\\src\\main\\resources\\static\\mum_baby_trade_history.csv")).build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
                .flatMap((FlatMapFunction<String, Tuple2<Long, Integer>>) (s, collector) -> {
                    String[] fields = s.split(",");
                    try {
                        long userId = Long.parseLong(fields[0]);
                        int amount = Integer.parseInt(fields[5]);
                        collector.collect(Tuple2.of(userId, amount));
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number format: " + fields[0] + ", " + fields[5]);
                    }
                }).returns(Types.TUPLE(Types.LONG, Types.INT))
                .keyBy(kv -> kv.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .print();

        env.execute();
    }

    @Resource
    private KafkaProducer kafkaProducer;

    @Test
    public void testKafkaSource() throws Exception {
        // 生产消息
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                kafkaProducer.sendMessage("test", "hello world " + i);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // 使用Flink消费消息
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("my_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                        .print();

        env.execute();
    }

}
