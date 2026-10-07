package com.ddd.dubbo.domain.common.event;

import com.ddd.dubbo.domain.common.time.TimeZoneUtils;

import java.io.Serializable;
import java.util.UUID;

public abstract class DomainEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String eventId;
    private final Long occurredAt;   // UTC 毫秒时间戳

    public DomainEvent() {
        this.eventId = UUID.randomUUID().toString().replace("-", "");
        this.occurredAt = TimeZoneUtils.getCurrentUtcTimestamp();
    }

    /** 事件类型（Outbox event_type 字段，也是 MQ 投递的 routing key） */
    public abstract String eventType();

    /** 事件负载（序列化为字符串存入 Outbox payload） */
    public abstract String payload();

    public String getEventId() { return eventId; }
    public Long getOccurredAt() { return occurredAt; }
}
