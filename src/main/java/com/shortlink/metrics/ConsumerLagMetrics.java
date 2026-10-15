package com.shortlink.metrics;

import com.shortlink.repository.ConsumerOffsetRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费滞后指标 {@code shortlink_consume_lag{partition}}。
 *
 * <p><b>为什么不用 Kafka 自身的 LAG？</b>本项目的消费进度以 MySQL 的
 * {@code t_consumer_offset} 为真相源（位点与业务数据同事务提交，见 ClickStatsConsumer）。
 * Kafka 侧提交位点会滞后甚至虚高——分区分配时若用 DB 位点 seek 且此后没有新消息，
 * 容器不会提交位点，broker 侧 {@code LAG} 就会一直显示一个并不存在的积压（「假 lag」）。
 * 直接拿它做告警会引发误报风暴，所以这里用「log-end offset − DB 位点」重新计算。
 *
 * <p>滞后值天然非负：位点可能领先于日志末端（topic 被重建/清理），此时按 0 计。
 */
@Component
public class ConsumerLagMetrics {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLagMetrics.class);
    private static final String LAG_METRIC = "shortlink.consume.lag";

    private final ConsumerOffsetRepository offsetRepository;
    private final MeterRegistry registry;
    private final KafkaProperties kafkaProperties;
    private final String groupId;
    private final String topic;

    /** 各分区的滞后值。Gauge 内部持弱引用，必须由本 map 强引用住 AtomicLong */
    private final Map<Integer, AtomicLong> lagByPartition = new ConcurrentHashMap<>();

    private volatile AdminClient adminClient;

    public ConsumerLagMetrics(ConsumerOffsetRepository offsetRepository,
                              MeterRegistry registry,
                              KafkaProperties kafkaProperties,
                              @Value("${spring.kafka.consumer.group-id}") String groupId,
                              @Value("${shortlink.topic.clicks}") String topic) {
        this.offsetRepository = offsetRepository;
        this.registry = registry;
        this.kafkaProperties = kafkaProperties;
        this.groupId = groupId;
        this.topic = topic;
    }

    @PostConstruct
    void init() {
        try {
            // 本项目是 PLAINTEXT，只传 bootstrap-servers 即可；
            // 若将来启用 SASL/SSL，需改用 KafkaProperties#buildAdminProperties 带上安全配置。
            Map<String, Object> props = Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                    AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
            this.adminClient = AdminClient.create(props);
        } catch (Exception e) {
            logger.warn("[LagMetrics] Kafka AdminClient 初始化失败，消费滞后指标不可用: {}", e.getMessage());
        }
    }

    @PreDestroy
    void close() {
        AdminClient admin = this.adminClient;
        if (admin != null) {
            admin.close(Duration.ofSeconds(2));
        }
    }

    @Scheduled(fixedDelayString = "${shortlink.metrics.lag-refresh-millis:30000}")
    public void refresh() {
        AdminClient admin = this.adminClient;
        if (admin == null) {
            return;
        }
        try {
            List<TopicPartition> partitions = new ArrayList<>();
            admin.describeTopics(List.of(topic)).allTopicNames().get()
                    .get(topic).partitions()
                    .forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            if (partitions.isEmpty()) {
                return;
            }

            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            partitions.forEach(tp -> request.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> logEnds =
                    admin.listOffsets(request).all().get();

            Map<Integer, Long> committed = new HashMap<>();
            offsetRepository.findByGroupIdAndTopic(groupId, topic)
                    .forEach(o -> committed.put(o.getPartitionNo(), o.getNextOffset()));

            for (TopicPartition tp : partitions) {
                long logEnd = logEnds.get(tp).offset();
                long next = committed.getOrDefault(tp.partition(), 0L);
                setLag(tp.partition(), Math.max(0L, logEnd - next));
            }
        } catch (Exception e) {
            // 指标采集失败绝不能影响业务，只记日志
            logger.debug("[LagMetrics] 刷新消费滞后失败（不影响业务）: {}", e.getMessage());
        }
    }

    private void setLag(int partition, long lag) {
        lagByPartition.computeIfAbsent(partition, p -> {
            AtomicLong ref = new AtomicLong();
            Gauge.builder(LAG_METRIC, ref, AtomicLong::get)
                    .description("消费滞后 = Kafka log-end offset − t_consumer_offset.next_offset（真相源）")
                    .tag("partition", String.valueOf(p))
                    .register(registry);
            return ref;
        }).set(lag);
    }
}
