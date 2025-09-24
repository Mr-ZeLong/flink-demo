package com.logilong.flink;

import com.logilong.flink.producer.KafkaProducer;
import jakarta.annotation.Resource;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlinkDemoApplication {
    @Resource
    private KafkaProducer kafkaProducer;
    public static void main(String[] args) {
        SpringApplication.run(FlinkDemoApplication.class, args);
    }

}
