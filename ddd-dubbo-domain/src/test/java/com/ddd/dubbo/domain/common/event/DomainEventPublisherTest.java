package com.ddd.dubbo.domain.common.event;

import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DomainEventPublisherTest {

    static class TestEvent extends DomainEvent {
        @Override
        public String eventType() { return "TestEvent"; }
        @Override
        public String payload() { return "{\"x\":1}"; }
    }

    @Test
    void publishAsyncWritesOutbox() {
        ApplicationEventPublisher appPublisher = mock(ApplicationEventPublisher.class);
        OutboxRepository outboxRepository = mock(OutboxRepository.class);
        DomainEventPublisher publisher = new DomainEventPublisher(appPublisher, outboxRepository);

        publisher.publishAsync(new TestEvent());

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertEquals("TestEvent", captor.getValue().getEventType());
        assertEquals(OutboxEvent.STATUS_PENDING, captor.getValue().getStatus());
    }

    @Test
    void publishSyncPublishesSpringEvent() {
        ApplicationEventPublisher appPublisher = mock(ApplicationEventPublisher.class);
        OutboxRepository outboxRepository = mock(OutboxRepository.class);
        DomainEventPublisher publisher = new DomainEventPublisher(appPublisher, outboxRepository);

        TestEvent event = new TestEvent();
        publisher.publishSync(event);
        verify(appPublisher).publishEvent(event);
    }
}
