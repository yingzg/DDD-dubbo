package com.ddd.dubbo.api.order.provider;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.api.order.model.req.CreateOrderReq;
import com.ddd.dubbo.api.order.model.req.QueryOrderReq;
import com.ddd.dubbo.api.order.model.resp.OrderResp;

public interface OrderProvider {
    Result<OrderResp> createOrder(CreateOrderReq req);
    Result<OrderResp> queryOrder(QueryOrderReq req);
}
