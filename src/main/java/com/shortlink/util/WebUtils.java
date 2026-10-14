package com.shortlink.util;

import jakarta.servlet.http.HttpServletRequest;

public final class WebUtils {

    private WebUtils() {
    }

    /** 优先取 X-Forwarded-For 首个 IP（反向代理场景） */
    public static String clientIp(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            return xff.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
