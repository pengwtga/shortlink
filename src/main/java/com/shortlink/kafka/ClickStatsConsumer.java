package com.shortlink.kafka;

import com.shortlink.repository.ConsumerOffsetRepository;
import com.shortlink.service.StatsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 点击事件消费端：批量拉取 -> 内存聚合 (code, date) -> 原子累加落库。
 * 高并发下将 N 次点击合并为每批每短链 1 次 UPSERT，大幅降低 MySQL 写压力。
 *
 * <p><b>不重复计数（幂等）</b>：Kafka 自身位点与「写库」之间存在窗口——写库成功但位点未提交时
 * 崩溃/重平衡，该批次会被重放，而 {@code clicks = clicks + VALUES(clicks)} 是累加语义，
 * 重放即重复计数。这里的做法是把「消费位点」也落到 MySQL，并与业务写库放在
 * <b>同一个事务</b>里提交：
 * <ul>
 *   <li>事务提交 → 数据与位点一起生效，位点已越过该批次，不会被重放；</li>
 *   <li>事务回滚 → 数据与位点一起撤销，下次从原位点重来，也不会有半批数据。</li>
 * </ul>
 * 分区分配时再从 DB 位点 seek（覆盖 Kafka 已提交位点），因此重复消费被彻底消除。
 * 代价仅是每批 1 行聚合 + 每分区 1 行位点，不影响批量聚合带来的削峰效果。
 */
@Component
public class ClickStatsConsumer implements ConsumerSeekAware {

    private static final Logger logger = LoggerFactory.getLogger(ClickStatsConsumer.class);

    private final StatsService statsService;
    private final ConsumerOffsetRepository offsetRepository;
    private final TransactionTemplate transactionTemplate;
    private final String groupId;

    public ClickStatsConsumer(StatsService statsService,
                              ConsumerOffsetRepository offsetRepository,
                              PlatformTransactionManager transactionManager,
                              @Value("${spring.kafka.consumer.group-id}") String groupId) {
        this.statsService = statsService;
        this.offsetRepository = offsetRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.groupId = groupId;
    }

    @KafkaListener(topics = "${shortlink.topic.clicks}")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        Map<String, long[]> agg = new HashMap<>();
        Map<TopicPartition, Long> maxOffsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            ClickEvent event = statsService.parseEvent(record.value());
            if (event != null) {
                String key = event.code() + "|" + event.date();
                agg.computeIfAbsent(key, k -> new long[2])[0] += 1;
            }
            // 脏消息跳过（不计入统计），但位点仍要推进，否则会卡住整个分区
            maxOffsets.merge(new TopicPartition(record.topic(), record.partition()),
                    record.offset(), Math::max);
        }

        // 一个事务内：累加统计 + 推进位点。任一失败则整体回滚，不会出现「数据写了位点没动」
        transactionTemplate.executeWithoutResult(status -> {
            agg.forEach((key, count) -> {
                String[] parts = key.split("\\|", 2);
                statsService.persistDailyClicks(parts[0], LocalDate.parse(parts[1]), count[0]);
            });
            maxOffsets.forEach((tp, offset) ->
                    offsetRepository.upsertOffset(groupId, tp.topic(), tp.partition(), offset + 1));
        });

        logger.debug("[ClickConsumer] batch={} rows={} partitions={}",
                records.size(), agg.size(), maxOffsets.size());
    }

    /**
     * 分区分配完成后，用 DB 里的位点覆盖 Kafka 自身位点。
     * DB 中还没有位点（首次启动）时不 seek，回落到 auto-offset-reset / 已提交位点。
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> offsetRepository
                .findByGroupIdAndTopicAndPartitionNo(groupId, tp.topic(), tp.partition())
                .ifPresent(stored -> {
                    logger.info("[ClickConsumer] 从 DB 位点恢复 {} -> {}", tp, stored.getNextOffset());
                    callback.seek(tp.topic(), tp.partition(), stored.getNextOffset());
                }));
    }
}
