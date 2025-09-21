package com.logilong.flink;

import com.logilong.flink.producer.KafkaProducer;
import jakarta.annotation.Resource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.Serial;
import java.io.Serializable;

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

    @Test
    public void unionTest() throws Exception {
        KeyedStream<Tuple2<String, Integer>, String> keyStream1 = env.fromData(Tuple2.of("hello", 1), Tuple2.of("world", 1), Tuple2.of("hello", 1))
                .keyBy(kv -> kv.f0);

        KeyedStream<Tuple2<String, Integer>, String> keyStream2 = env.fromData(Tuple2.of("hello", 1), Tuple2.of("flink", 1), Tuple2.of("flink", 1))
                .keyBy(kv -> kv.f0);

        // union 后前面的 keyBy 就失效了，需要重新 keyBy
        keyStream1.union(keyStream2)
                        .keyBy(kv -> kv.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                        .print();

        env.execute();
    }

    @Test
    public void connectCoProcessTest() throws Exception{
        User user1 = new User(1L, "张三", 0), user2 = new User(2L, "李四", 0);
        Order order1 = new Order(1L, 1L, "待支付"), order2 = new Order(2L, 2L, "待支付");

        MapStateDescriptor<Long, User> descriptor = new MapStateDescriptor<>("userInfoState", Long.class, User.class);
        BroadcastStream<User> broadcastStream = env.fromData(user1, user2)
                .broadcast(descriptor);

        env.fromData(order1, order2)
                .connect(broadcastStream)
                        .process(new BroadcastProcessFunction<Order, User, OrderInfo>() {
                            @Override
                            public void processElement(Order value, BroadcastProcessFunction<Order, User, OrderInfo>.ReadOnlyContext ctx, Collector<OrderInfo> out) throws Exception {
                                ReadOnlyBroadcastState<Long, User> broadcastState = ctx.getBroadcastState(descriptor);
                                out.collect(new OrderInfo(value.getOrderId(), broadcastState.get(value.getUserId()), value.getStatus()));
                            }
                            @Override
                            public void processBroadcastElement(User value, BroadcastProcessFunction<Order, User, OrderInfo>.Context ctx, Collector<OrderInfo> out) throws Exception {
                                BroadcastState<Long, User> broadcastState = ctx.getBroadcastState(descriptor);
                                broadcastState.put(value.getUserId(), value);
                            }
                        })
                .keyBy(orderInfo -> orderInfo.getUser().getGender())
                .print();

        env.execute();
    }



}

@Data
@AllArgsConstructor
class OrderInfo implements Serializable{
    @Serial
    private static final long serialVersionUID = 134665467L;
    private Long orderId;
    private User user;
    private String status;
}


@Data
@AllArgsConstructor
class Order implements Serializable {
    @Serial
    private static final long serialVersionUID = 134665234467L;
    private Long orderId;
    private Long userId;
    private String status;
}
@Data
@AllArgsConstructor
class User implements Serializable {
    @Serial
    private static final long serialVersionUID = 123434665467L;
    private Long userId;
    private String name;
    private Integer gender;
}