package com.ddd.dubbo.domain.order.statemachine;

import java.math.BigDecimal;

public class OrderContext {
    private BigDecimal balance;
    private BigDecimal amount;

    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
