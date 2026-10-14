package com.shortlink.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 点击事件生产者：跳转链路只做投递（异步），统计由消费端异步完成，
 * 保证重定向延迟不受统计写入影响。以 code 为 key 保证同一短链有序。
 */
@Component
public class ClickEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(ClickEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public ClickEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                              ObjectMapper objectMapper,
                              @Value("${shortlink.topic.clicks}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void send(ClickEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, event.code(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.warn("[ClickProducer] send failed: code={} err={}",
                                    event.code(), ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            // 统计丢失可容忍，不能影响跳转
            logger.warn("[ClickProducer] serialize failed: {}", e.getMessage());
        }
    }
}
