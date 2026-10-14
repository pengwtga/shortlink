package com.shortlink.kafka;

/**
 * 点击事件（Kafka 消息体，JSON 序列化）
 */
public record ClickEvent(
        String code,
        long timestamp,
        String ip,
        String userAgent,
        String referer,
        String date
) {
}
