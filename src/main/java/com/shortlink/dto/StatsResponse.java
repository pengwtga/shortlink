package com.shortlink.dto;

import java.time.LocalDate;
import java.util.List;

public record StatsResponse(
        String code,
        long totalClicks,
        long todayClicks,
        long uv,
        List<DailyStat> daily
) {

    public record DailyStat(LocalDate date, long clicks) {
    }
}
