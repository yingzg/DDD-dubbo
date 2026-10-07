package com.ddd.dubbo.domain.common.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** 时区转换工具：统一存储 UTC 毫秒时间戳，展示层按用户时区转换 */
public final class TimeZoneUtils {

    private TimeZoneUtils() {
    }

    public static long getCurrentUtcTimestamp() {
        return Instant.now().toEpochMilli();
    }

    /** 时间戳转指定时区的格式化字符串 */
    public static String formatTimestamp(long timestamp, String timezoneId, String pattern) {
        if (timestamp <= 0 || isBlank(timezoneId) || isBlank(pattern)) {
            return "";
        }
        try {
            Instant instant = Instant.ofEpochMilli(timestamp);
            ZoneId zone = ZoneId.of(timezoneId);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
            return localDateTime.format(DateTimeFormatter.ofPattern(pattern));
        } catch (Exception e) {
            return "";
        }
    }

    /** 指定时区的字符串时间转 UTC 时间戳 */
    public static long parseToTimestamp(String timeStr, String timezoneId, String pattern) {
        if (isBlank(timeStr) || isBlank(timezoneId) || isBlank(pattern)) {
            return 0L;
        }
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern(pattern));
            return localDateTime.atZone(ZoneId.of(timezoneId)).toInstant().toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
