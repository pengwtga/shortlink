package com.shortlink.service;

import com.shortlink.dto.CreateLinkRequest;
import com.shortlink.dto.LinkResponse;
import com.shortlink.entity.Link;
import com.shortlink.id.Base62;
import com.shortlink.id.SnowflakeIdGenerator;
import com.shortlink.repository.LinkRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 短链生成与解析
 *
 * 缓存设计（key: sl:link:{code}，value: "{expireMillis}|{longUrl}"）：
 * - 命中直接返回，跳过 DB
 * - 未命中回源 MySQL 并回填缓存，TTL 24h
 * - 不存在的短码写入空值占位（TTL 5min），防止恶意短码穿透缓存打垮 DB
 */
@Service
public class LinkService {

    private static final String CACHE_KEY_PREFIX = "sl:link:";
    private static final String NULL_PLACEHOLDER = "__NULL__";

    private final LinkRepository linkRepository;
    private final StringRedisTemplate redis;
    private final SnowflakeIdGenerator idGenerator;
    private final String baseUrl;
    private final long cacheTtlHours;

    public LinkService(LinkRepository linkRepository,
                       StringRedisTemplate redis,
                       SnowflakeIdGenerator idGenerator,
                       @Value("${shortlink.base-url}") String baseUrl,
                       @Value("${shortlink.cache.ttl-hours}") long cacheTtlHours) {
        this.linkRepository = linkRepository;
        this.redis = redis;
        this.idGenerator = idGenerator;
        this.baseUrl = baseUrl;
        this.cacheTtlHours = cacheTtlHours;
    }

    @Transactional
    public LinkResponse create(CreateLinkRequest request) {
        String longUrl = request.longUrl().trim();
        if (!longUrl.startsWith("http://") && !longUrl.startsWith("https://")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "longUrl 必须以 http:// 或 https:// 开头");
        }
        if (request.expireAt() != null && request.expireAt().isBefore(LocalDateTime.now())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "expireAt 不能早于当前时间");
        }

        long id = idGenerator.nextId();
        String code = Base62.encode(id);

        Link link = new Link();
        link.setId(id);
        link.setCode(code);
        link.setLongUrl(longUrl);
        link.setExpireAt(request.expireAt());
        linkRepository.save(link);

        redis.opsForValue().set(cacheKey(code), toCacheValue(longUrl, request.expireAt()),
                Duration.ofHours(cacheTtlHours));

        return new LinkResponse(code, baseUrl + "/" + code, longUrl, request.expireAt());
    }

    /** 解析短码对应的长链接；不存在抛 404，已过期/禁用抛 410 */
    public String resolve(String code) {
        String key = cacheKey(code);
        String cached = redis.opsForValue().get(key);

        if (cached == null) {
            Link link = linkRepository.findByCode(code).orElse(null);
            if (link == null || link.getStatus() == 0) {
                redis.opsForValue().set(key, NULL_PLACEHOLDER, Duration.ofMinutes(5));
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "短链不存在: " + code);
            }
            cached = toCacheValue(link.getLongUrl(), link.getExpireAt());
            redis.opsForValue().set(key, cached, Duration.ofHours(cacheTtlHours));
        } else if (NULL_PLACEHOLDER.equals(cached)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "短链不存在: " + code);
        }

        String[] parts = cached.split("\\|", 2);
        long expireMillis = Long.parseLong(parts[0]);
        if (expireMillis > 0 && expireMillis < System.currentTimeMillis()) {
            throw new ResponseStatusException(HttpStatus.GONE, "短链已过期: " + code);
        }
        return parts[1];
    }

    public Link getOrThrow(String code) {
        return linkRepository.findByCode(code)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "短链不存在: " + code));
    }

    private String cacheKey(String code) {
        return CACHE_KEY_PREFIX + code;
    }

    private String toCacheValue(String longUrl, LocalDateTime expireAt) {
        // expireAt 是「本地墙上时间」，必须按 JVM 默认时区解释成瞬时时刻，
        // 否则与 System.currentTimeMillis()（绝对 UTC）比较会产生时区偏移。
        long expireMillis = expireAt == null ? 0 : expireAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return expireMillis + "|" + longUrl;
    }
}
