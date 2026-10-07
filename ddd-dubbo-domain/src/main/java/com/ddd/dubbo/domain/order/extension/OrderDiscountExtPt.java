package com.ddd.dubbo.domain.order.extension;

import com.ddd.dubbo.domain.extension.ExtensionPoint;
import com.ddd.dubbo.domain.order.entity.Order;
import java.math.BigDecimal;

public interface OrderDiscountExtPt extends ExtensionPoint {
    BigDecimal calculateDiscount(Order order);
}
