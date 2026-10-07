package com.ddd.dubbo.domain.order.entity;

import com.ddd.dubbo.domain.order.statemachine.OrderContext;
import com.ddd.dubbo.domain.order.statemachine.OrderEvent;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import com.ddd.dubbo.domain.statemachine.StateMachine;
import com.ddd.dubbo.domain.statemachine.StateMachineBuilderFactory;

import java.math.BigDecimal;

public class Order {

    private static final StateMachine<OrderStatus, OrderEvent, OrderContext> ORDER_STATE_MACHINE = buildStateMachine();

    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount = BigDecimal.ZERO;
    private OrderStatus status;
    private Long createdAt;
    private Long updatedAt;

    private static StateMachine<OrderStatus, OrderEvent, OrderContext> buildStateMachine() {
        return StateMachineBuilderFactory.<OrderStatus, OrderEvent, OrderContext>create()
                .transition().from(OrderStatus.INIT).to(OrderStatus.PAID).on(OrderEvent.PAY)
                .when(ctx -> ctx.getBalance() != null && ctx.getAmount() != null
                        && ctx.getBalance().compareTo(ctx.getAmount()) >= 0)
                .build()
                .transition().from(OrderStatus.PAID).to(OrderStatus.SHIPPED).on(OrderEvent.SHIP).build()
                .transition().from(OrderStatus.SHIPPED).to(OrderStatus.COMPLETED).on(OrderEvent.COMPLETE).build()
                .transition().from(OrderStatus.INIT).to(OrderStatus.CANCELLED).on(OrderEvent.CANCEL).build()
                .build();
    }

    public void applyDiscount(BigDecimal d) {
        if (d != null) {
            this.discount = this.discount.add(d);
        }
    }

    /** 支付：走状态机 INIT -> PAID（余额校验在状态机 when 条件里） */
    public void pay(OrderContext ctx) {
        this.status = ORDER_STATE_MACHINE.fireEvent(this.status, OrderEvent.PAY, ctx);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public Long getCreatedAt() { return createdAt; }
    public void setCreatedAt(Long createdAt) { this.createdAt = createdAt; }
    public Long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Long updatedAt) { this.updatedAt = updatedAt; }
}
