package com.logilong.flink.producer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaProducer {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.info("send message success: {}", result);
                            }else {
                                log.error("send message error: {}", ex.getMessage());
                            }
                        });
    }
}
