package com.ddd.dubbo.domain.common.time;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TimeZoneUtilsTest {

    @Test
    void formatTimestampToShanghai() {
        long utc = 1640995200000L; // 2022-01-01 00:00:00 UTC
        assertEquals("2022-01-01 08:00:00", TimeZoneUtils.formatTimestamp(utc, "Asia/Shanghai", "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    void formatTimestampToUtc() {
        long utc = 1640995200000L;
        assertEquals("2022-01-01 00:00:00", TimeZoneUtils.formatTimestamp(utc, "UTC", "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    void emptyWhenInvalid() {
        assertEquals("", TimeZoneUtils.formatTimestamp(0L, "UTC", "yyyy-MM-dd"));
        assertEquals("", TimeZoneUtils.formatTimestamp(1640995200000L, "", "yyyy-MM-dd"));
    }

    @Test
    void getCurrentUtcTimestampPositive() {
        assertTrue(TimeZoneUtils.getCurrentUtcTimestamp() > 0);
    }
}
