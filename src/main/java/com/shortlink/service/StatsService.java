package com.shortlink.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shortlink.dto.StatsResponse;
import com.shortlink.kafka.ClickEvent;
import com.shortlink.kafka.ClickEventProducer;
import com.shortlink.repository.LinkStatsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

/**
 * 点击统计
 *
 * 写入路径（跳转时）：
 * - Redis INCR 当日实时计数：sl:cnt:{code}:{date}（TTL 48h）
 * - Redis HyperLogLog UV：sl:uv:{code}（总量，滑动 TTL 180 天）、sl:uv:{code}:{date}（当日，TTL 48h）
 * - Kafka 异步投递点击事件 -> 消费端聚合成日统计落库 MySQL
 *
 * 读取路径：总点击查 MySQL 日统计求和（持久、有界、不受 Redis 淘汰影响），
 * 今日点击与 UV 查 Redis（实时），Redis 缺失时回退 MySQL。
 *
 * 注意：Redis 在 docker-compose 中配置为 volatile-lru，所有业务 key 都必须带 TTL，
 * 否则内存打满时会退化为 OOM 写拒绝；同时不得存在「无 TTL 且必须长期保留」的统计 key。
 */
@Service
public class StatsService {

    private static final String CNT_KEY_PREFIX = "sl:cnt:";
    private static final String UV_KEY_PREFIX = "sl:uv:";

    /** 总 UV 的滑动 TTL：有流量则续期，长期无流量则随 LRU / 过期释放内存 */
    private static final Duration UV_TOTAL_TTL = Duration.ofDays(180);

    private static final Logger logger = LoggerFactory.getLogger(StatsService.class);

    private final StringRedisTemplate redis;
    private final LinkStatsRepository statsRepository;
    private final ClickEventProducer producer;
    private final ObjectMapper objectMapper;

    public StatsService(StringRedisTemplate redis,
                        LinkStatsRepository statsRepository,
                        ClickEventProducer producer,
                        ObjectMapper objectMapper) {
        this.redis = redis;
        this.statsRepository = statsRepository;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    /** 跳转时记录一次点击（Redis 同步计数 + Kafka 异步事件） */
    public void recordClick(String code, String ip, String userAgent, String referer) {
        LocalDate today = LocalDate.now();

        // 只保留「当日」实时计数：固定 48h TTL，值随窗口自然过期。
        // 历史总量不在这里累加，由 MySQL 日统计求和得出（见 getStats）。
        incrementWithTtl(CNT_KEY_PREFIX + code + ":" + today, Duration.ofHours(48));

        // 总 UV：MySQL 未持久化 UV，只能依赖 HLL。用滑动 TTL 兜底内存增长——
        // 有流量就续期，长期无流量的短链自动释放（LRU 也会优先淘汰这类冷 key）。
        redis.opsForHyperLogLog().add(UV_KEY_PREFIX + code, ip);
        redis.expire(UV_KEY_PREFIX + code, UV_TOTAL_TTL);

        redis.opsForHyperLogLog().add(UV_KEY_PREFIX + code + ":" + today, ip);
        redis.expire(UV_KEY_PREFIX + code + ":" + today, Duration.ofHours(48));

        producer.send(new ClickEvent(code, System.currentTimeMillis(), ip,
                userAgent != null ? userAgent : "", referer != null ? referer : "", today.toString()));
    }

    public StatsResponse getStats(String code) {
        LocalDate today = LocalDate.now();

        List<StatsResponse.DailyStat> daily = statsRepository
                .findByCodeAndStatDateBetweenOrderByStatDateAsc(code, today.minusDays(6), today)
                .stream()
                .map(s -> new StatsResponse.DailyStat(s.getStatDate(), s.getClicks()))
                .toList();

        // 总点击 = MySQL 历史汇总 + Kafka 尚未落库的今日增量
        long mysqlTotal = statsRepository.sumClicksByCode(code);
        long mysqlToday = daily.stream()
                .filter(d -> today.equals(d.date()))
                .mapToLong(StatsResponse.DailyStat::clicks)
                .findFirst()
                .orElse(0L);

        // 今日点击以 Redis 实时值为准（Kafka 落库有秒级延迟）；
        // Redis 计数丢失时回退到 MySQL，至少不会显示为 0。
        long redisToday = longValue(redis.opsForValue().get(CNT_KEY_PREFIX + code + ":" + today));
        long todayClicks = Math.max(redisToday, mysqlToday);
        long totalClicks = mysqlTotal + Math.max(0L, redisToday - mysqlToday);

        Long uv = redis.opsForHyperLogLog().size(UV_KEY_PREFIX + code);

        return new StatsResponse(code, totalClicks, todayClicks, uv == null ? 0 : uv, daily);
    }

    /** Kafka 消费端调用：批量聚合后按 (code, date) 原子累加落库 */
    @Transactional
    public void persistDailyClicks(String code, LocalDate date, long clicks) {
        statsRepository.upsertClicks(code, date, clicks);
    }

    /** 反序列化单条消息，失败返回 null 由调用方跳过 */
    public ClickEvent parseEvent(String json) {
        try {
            return objectMapper.readValue(json, ClickEvent.class);
        } catch (Exception e) {
            logger.warn("[StatsService] malformed click event: {}", e.getMessage());
            return null;
        }
    }

    private void incrementWithTtl(String key, Duration ttl) {
        Long v = redis.opsForValue().increment(key);
        if (v != null && v == 1L) {
            redis.expire(key, ttl);
        }
    }

    private long longValue(String v) {
        return v == null ? 0 : Long.parseLong(v);
    }
}
