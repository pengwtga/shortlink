package com.shortlink.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka Topic 初始化：点击事件主题，3 分区保证消费吞吐
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic clicksTopic(@Value("${shortlink.topic.clicks}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 3600 * 1000))
                .build();
    }
}
