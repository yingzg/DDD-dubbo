package com.ddd.dubbo.domain.common.event;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/** 领域事件发布器（同步走 Spring 事件，异步走 MQ，MQ 模板在 app 层按需注入） */
@Component
public class DomainEventPublisher {

    private final ApplicationEventPublisher applicationEventPublisher;

    public DomainEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /** 同步发布（同进程监听者立即处理） */
    public void publishSync(DomainEvent event) {
        applicationEventPublisher.publishEvent(event);
    }
}
