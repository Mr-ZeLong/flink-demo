package com.logilong.flink.config;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfig {

    @Bean
    public StreamExecutionEnvironment initStreamEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        return env;
    }

    @Bean
    public StreamTableEnvironment initTableEnv(StreamExecutionEnvironment env){
        return StreamTableEnvironment.create(env);
    }
}
