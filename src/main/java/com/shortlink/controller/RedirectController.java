package com.shortlink.controller;

import com.shortlink.service.LinkService;
import com.shortlink.service.RateLimitService;
import com.shortlink.service.StatsService;
import com.shortlink.util.WebUtils;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

/**
 * 短链跳转：限流 -> 读缓存/DB 解析 -> 记录点击(Redis+Kafka) -> 302 跳转
 *
 * 用 302 而非 301：301 会被浏览器永久缓存，后续点击不再到达服务端，统计将严重偏低。
 */
@RestController
public class RedirectController {

    private final LinkService linkService;
    private final StatsService statsService;
    private final RateLimitService rateLimitService;
    private final int redirectLimit;
    private final int redirectWindowSeconds;

    public RedirectController(LinkService linkService,
                              StatsService statsService,
                              RateLimitService rateLimitService,
                              @Value("${shortlink.rate-limit.redirect-limit}") int redirectLimit,
                              @Value("${shortlink.rate-limit.redirect-window-seconds}") int redirectWindowSeconds) {
        this.linkService = linkService;
        this.statsService = statsService;
        this.rateLimitService = rateLimitService;
        this.redirectLimit = redirectLimit;
        this.redirectWindowSeconds = redirectWindowSeconds;
    }

    @GetMapping("/{code:[0-9A-Za-z]{6,12}}")
    public ResponseEntity<Void> redirect(@PathVariable String code, HttpServletRequest request) {
        rateLimitService.check("redirect", WebUtils.clientIp(request),
                redirectLimit, redirectWindowSeconds);

        String longUrl = linkService.resolve(code);
        statsService.recordClick(code, WebUtils.clientIp(request),
                request.getHeader("User-Agent"), request.getHeader("Referer"));

        return ResponseEntity.status(302).location(URI.create(longUrl)).build();
    }
}
