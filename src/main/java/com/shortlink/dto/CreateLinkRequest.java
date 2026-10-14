package com.shortlink.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.LocalDateTime;

public record CreateLinkRequest(
        @NotBlank(message = "longUrl 不能为空")
        @Size(max = 2048, message = "longUrl 过长")
        String longUrl,
        LocalDateTime expireAt
) {
}
