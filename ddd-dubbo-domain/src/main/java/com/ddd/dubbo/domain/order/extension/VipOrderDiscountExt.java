package com.ddd.dubbo.domain.order.extension;

import com.ddd.dubbo.domain.extension.Extension;
import com.ddd.dubbo.domain.order.entity.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;

@Extension(bizId = "vip", useCase = "order", scenario = "default")
@Component
public class VipOrderDiscountExt implements OrderDiscountExtPt {
    @Override
    public BigDecimal calculateDiscount(Order order) {
        return order.getAmount().multiply(new BigDecimal("0.2"));
    }
}
