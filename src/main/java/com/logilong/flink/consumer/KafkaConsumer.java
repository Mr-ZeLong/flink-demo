package com.logilong.flink.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {
    @KafkaListener(topics = "test", groupId = "test-consumer-group")
    public void listen(String message) {
        System.out.println( message);
    }

    @KafkaListener(topics = "test")
    public void listen(ConsumerRecord<String, String> record) {
        String message = record.value();
        String topic = record.topic();
        long offset = record.offset();
        System.out.printf("Consumed message [%s] from topic [%s], offset [%d]%n", message, topic, offset);
        // 在这里处理你的业务逻辑
    }
}
