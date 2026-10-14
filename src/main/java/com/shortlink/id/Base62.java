package com.shortlink.id;

/**
 * Base62 编解码：短码字符集 0-9 a-z A-Z
 * 63 位雪花 ID -> 11 位短码
 */
public final class Base62 {

    private static final char[] ALPHABET =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int BASE = ALPHABET.length;

    private Base62() {
    }

    public static String encode(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative: " + value);
        }
        if (value == 0) {
            return "0";
        }
        StringBuilder sb = new StringBuilder(11);
        while (value > 0) {
            sb.append(ALPHABET[(int) (value % BASE)]);
            value /= BASE;
        }
        return sb.toString();
    }

    public static long decode(String code) {
        long value = 0;
        for (char c : code.toCharArray()) {
            int digit = indexOf(c);
            if (digit < 0) {
                throw new IllegalArgumentException("invalid base62 char: " + c);
            }
            value = value * BASE + digit;
        }
        return value;
    }

    private static int indexOf(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'z') return c - 'a' + 10;
        if (c >= 'A' && c <= 'Z') return c - 'A' + 36;
        return -1;
    }
}
