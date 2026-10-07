package com.ddd.dubbo.infra.order.mapper;

import com.ddd.dubbo.infra.order.dataobject.OrderDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderMapper {
    OrderDO findById(@Param("id") Long id);
    int insert(OrderDO orderDO);
}
