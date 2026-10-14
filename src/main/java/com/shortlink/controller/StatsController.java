package com.shortlink.controller;

import com.shortlink.dto.StatsResponse;
import com.shortlink.service.LinkService;
import com.shortlink.service.StatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 短链统计接口
 */
@RestController
@RequestMapping("/api/v1/stats")
public class StatsController {

    private final LinkService linkService;
    private final StatsService statsService;

    public StatsController(LinkService linkService, StatsService statsService) {
        this.linkService = linkService;
        this.statsService = statsService;
    }

    /** 查询短链统计：总点击/今日点击/UV + 近 7 天日点击明细 */
    @GetMapping("/{code}")
    public StatsResponse stats(@PathVariable String code) {
        linkService.getOrThrow(code); // 确保短链存在，否则 404
        return statsService.getStats(code);
    }
}
