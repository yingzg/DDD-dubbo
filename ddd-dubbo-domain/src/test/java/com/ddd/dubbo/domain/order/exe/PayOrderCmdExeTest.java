package com.ddd.dubbo.domain.order.exe;

import com.ddd.dubbo.domain.common.event.DomainEventPublisher;
import com.ddd.dubbo.domain.common.exception.BizException;
import com.ddd.dubbo.domain.order.command.PayOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PayOrderCmdExeTest {

    @Test
    void payMovesStatusAndPublishesEvent() {
        OrderRepository repo = mock(OrderRepository.class);
        DomainEventPublisher publisher = mock(DomainEventPublisher.class);
        Order order = new Order();
        order.setId(1L);
        order.setStatus(OrderStatus.INIT);
        order.setAmount(new BigDecimal("100"));
        when(repo.findById(1L)).thenReturn(order);

        PayOrderCmdExe exe = new PayOrderCmdExe();
        inject(exe, repo, publisher);

        PayOrderCommand cmd = new PayOrderCommand();
        cmd.setOrderId(1L);
        cmd.setBalance(new BigDecimal("100"));

        Order result = exe.execute(cmd);
        assertEquals(OrderStatus.PAID, result.getStatus());
        verify(publisher, times(1)).publishAsync(Mockito.any());
        verify(repo, times(1)).save(order);
    }

    @Test
    void payThrowsWhenOrderNotFound() {
        OrderRepository repo = mock(OrderRepository.class);
        DomainEventPublisher publisher = mock(DomainEventPublisher.class);
        when(repo.findById(1L)).thenReturn(null);

        PayOrderCmdExe exe = new PayOrderCmdExe();
        inject(exe, repo, publisher);

        PayOrderCommand cmd = new PayOrderCommand();
        cmd.setOrderId(1L);
        assertThrows(BizException.class, () -> exe.execute(cmd));
        verify(publisher, never()).publishAsync(Mockito.any());
    }

    private void inject(PayOrderCmdExe exe, OrderRepository repo, DomainEventPublisher publisher) {
        try {
            java.lang.reflect.Field repoField = PayOrderCmdExe.class.getDeclaredField("orderRepository");
            repoField.setAccessible(true);
            repoField.set(exe, repo);
            java.lang.reflect.Field pubField = PayOrderCmdExe.class.getDeclaredField("domainEventPublisher");
            pubField.setAccessible(true);
            pubField.set(exe, publisher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
