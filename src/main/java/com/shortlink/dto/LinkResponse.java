package com.shortlink.dto;

import java.time.LocalDateTime;

public record LinkResponse(
        String code,
        String shortUrl,
        String longUrl,
        LocalDateTime expireAt
) {
}
