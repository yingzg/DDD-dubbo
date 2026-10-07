package com.ddd.dubbo.domain.common.event;

import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class DomainEventPublisher {

    private final ApplicationEventPublisher applicationEventPublisher;
    private final OutboxRepository outboxRepository;

    public DomainEventPublisher(ApplicationEventPublisher applicationEventPublisher,
                                OutboxRepository outboxRepository) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.outboxRepository = outboxRepository;
    }

    /** 同步发布（同进程监听者立即处理，事务内联动） */
    public void publishSync(DomainEvent event) {
        applicationEventPublisher.publishEvent(event);
    }

    /** 异步发布：写 Outbox 本地消息表（与业务同事务），由 OutboxRelay 投递 MQ，最终一致 */
    public void publishAsync(DomainEvent event) {
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setEventType(event.eventType());
        outboxEvent.setPayload(event.payload());
        outboxEvent.setStatus(OutboxEvent.STATUS_PENDING);
        outboxEvent.setRetryCount(0);
        outboxRepository.save(outboxEvent);
    }
}
