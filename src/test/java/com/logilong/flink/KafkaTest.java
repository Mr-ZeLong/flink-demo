package com.logilong.flink;

import com.logilong.flink.producer.KafkaProducer;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;



@SpringBootTest
@Slf4j
public class KafkaTest {

    @Resource
    private KafkaProducer producer;

    @Test
    public void test() throws InterruptedException {
        String topic = "test";
        String message = "hello, kafka";



        for(int i = 0; i < 100; i++){
            producer.sendMessage(topic, message);
            Thread.sleep(2000);
        }
    }
}
