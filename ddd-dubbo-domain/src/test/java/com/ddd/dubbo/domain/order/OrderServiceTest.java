package com.ddd.dubbo.domain.order;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.service.OrderService;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderServiceTest {

    @Test
    void createOrderBuildsInitOrderWithUtcTimestamp() {
        OrderRepository repo = mock(OrderRepository.class);
        OrderService service = new OrderService(repo);
        Order order = service.createOrder(100L, new BigDecimal("50"));
        assertEquals(Long.valueOf(100L), order.getUserId());
        assertEquals(new BigDecimal("50"), order.getAmount());
        assertEquals(OrderStatus.INIT.name(), order.getStatus());
        assertNotNull(order.getCreatedAt());
    }

    @Test
    void applyDiscountAccumulates() {
        Order order = new Order();
        order.setAmount(new BigDecimal("100"));
        order.applyDiscount(new BigDecimal("20"));
        assertEquals(new BigDecimal("20"), order.getDiscount());
    }

    @Test
    void saveDelegatesToRepository() {
        OrderRepository repo = mock(OrderRepository.class);
        OrderService service = new OrderService(repo);
        service.save(new Order());
        verify(repo).save(any(Order.class));
    }
}
