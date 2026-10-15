package com.shortlink.service;

import com.shortlink.metrics.ShortlinkMetrics;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * 基于 Redis + Lua 的固定窗口限流器
 *
 * Lua 保证「INCR + 首次设置过期」的原子性，避免竞态导致 key 永不过期。
 */
@Service
public class RateLimitService {

    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT = new DefaultRedisScript<>(
            """
            local current = redis.call('INCR', KEYS[1])
            if current == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            if current > tonumber(ARGV[2]) then
                return 0
            end
            return 1
            """,
            Long.class);

    private final StringRedisTemplate redis;
    private final ShortlinkMetrics metrics;

    public RateLimitService(StringRedisTemplate redis, ShortlinkMetrics metrics) {
        this.redis = redis;
        this.metrics = metrics;
    }

    /**
     * @param scene          限流场景（create / redirect），用于隔离 key
     * @param identity       限流主体（通常为客户端 IP）
     * @param limit          窗口内允许的最大次数
     * @param windowSeconds  窗口长度（秒）
     */
    public void check(String scene, String identity, int limit, int windowSeconds) {
        String key = "sl:rl:" + scene + ":" + identity;
        Long allowed = redis.execute(RATE_LIMIT_SCRIPT,
                List.of(key),
                String.valueOf(windowSeconds),
                String.valueOf(limit));
        if (allowed == null || allowed == 0L) {
            metrics.rateLimitRejected(scene);
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "请求过于频繁，请稍后再试（限流: " + limit + " 次/" + windowSeconds + "s）");
        }
    }
}
