package com.shortlink.controller;

import com.shortlink.dto.CreateLinkRequest;
import com.shortlink.dto.LinkResponse;
import com.shortlink.entity.Link;
import com.shortlink.service.LinkService;
import com.shortlink.service.RateLimitService;
import com.shortlink.util.WebUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 短链管理接口
 */
@RestController
@RequestMapping("/api/v1/links")
public class LinkController {

    private final LinkService linkService;
    private final RateLimitService rateLimitService;
    private final int createLimit;
    private final int createWindowSeconds;

    public LinkController(LinkService linkService,
                          RateLimitService rateLimitService,
                          @Value("${shortlink.rate-limit.create-limit}") int createLimit,
                          @Value("${shortlink.rate-limit.create-window-seconds}") int createWindowSeconds) {
        this.linkService = linkService;
        this.rateLimitService = rateLimitService;
        this.createLimit = createLimit;
        this.createWindowSeconds = createWindowSeconds;
    }

    /** 创建短链（按 IP 限流，默认 5 次/分钟） */
    @PostMapping
    public LinkResponse create(@Valid @RequestBody CreateLinkRequest request,
                               HttpServletRequest httpRequest) {
        rateLimitService.check("create", WebUtils.clientIp(httpRequest),
                createLimit, createWindowSeconds);
        return linkService.create(request);
    }

    /** 查询短链详情 */
    @GetMapping("/{code}")
    public LinkResponse get(@PathVariable String code) {
        Link link = linkService.getOrThrow(code);
        return new LinkResponse(link.getCode(), null, link.getLongUrl(), link.getExpireAt());
    }
}
