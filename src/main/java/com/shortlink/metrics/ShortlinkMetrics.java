package com.shortlink.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * 业务指标出口。AIOps 侧依赖这些指标做异常检测与根因推断，
 * 因此指标语义必须稳定：<b>outcome 互斥且可累加</b>，即
 * {@code sum(shortlink_redirect_total)} 恒等于跳转请求总数。
 *
 * <p>命名遵循 Prometheus 约定——Micrometer 会把点号转成下划线，
 * 并给 Counter 追加 {@code _total} 后缀：
 * <ul>
 *   <li>{@code shortlink.redirect} → {@code shortlink_redirect_total{outcome}}</li>
 *   <li>{@code shortlink.ratelimit.rejected} → {@code shortlink_ratelimit_rejected_total{scene}}</li>
 * </ul>
 */
@Component
public class ShortlinkMetrics {

    /** 缓存命中，直接 302 */
    public static final String OUTCOME_HIT = "hit";
    /** 缓存未命中，回源 MySQL 命中并回填，302 */
    public static final String OUTCOME_MISS = "miss";
    /** 命中空值占位，未打 DB 直接 404 */
    public static final String OUTCOME_NULL_PLACEHOLDER = "null_placeholder";
    /** 缓存与 DB 均不存在（或已禁用），404 */
    public static final String OUTCOME_NOT_FOUND = "not_found";
    /** 已过期，410 */
    public static final String OUTCOME_EXPIRED = "expired";

    private final MeterRegistry registry;

    public ShortlinkMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** 记录一次跳转的最终结果。一次请求只会产生一个 outcome，避免重复计数。 */
    public void redirectOutcome(String outcome) {
        Counter.builder("shortlink.redirect")
                .description("短链跳转结果计数（按最终 outcome 互斥）")
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    /** 记录一次被限流拒绝的请求 */
    public void rateLimitRejected(String scene) {
        Counter.builder("shortlink.ratelimit.rejected")
                .description("被限流拒绝的请求数")
                .tag("scene", scene)
                .register(registry)
                .increment();
    }
}
