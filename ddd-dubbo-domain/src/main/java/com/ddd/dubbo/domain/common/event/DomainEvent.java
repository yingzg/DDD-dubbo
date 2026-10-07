package com.ddd.dubbo.domain.common.event;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/** 领域事件基类 */
public abstract class DomainEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private Date occurredAt;

    public DomainEvent() {
        this.eventId = UUID.randomUUID().toString().replace("-", "");
        this.occurredAt = new Date();
    }

    public String getEventId() { return eventId; }
    public Date getOccurredAt() { return occurredAt; }
}
