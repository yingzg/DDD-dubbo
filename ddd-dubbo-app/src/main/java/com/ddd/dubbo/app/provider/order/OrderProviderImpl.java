package com.ddd.dubbo.app.provider.order;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.api.order.model.req.CreateOrderReq;
import com.ddd.dubbo.api.order.model.req.QueryOrderReq;
import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.api.order.provider.OrderProvider;
import com.ddd.dubbo.app.config.exception.ExceptionAnnotation;
import com.ddd.dubbo.domain.exe.CommandBus;
import com.ddd.dubbo.domain.exe.QueryBus;
import com.ddd.dubbo.domain.order.command.CreateOrderCommand;
import com.ddd.dubbo.domain.order.command.QueryOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.infra.order.converter.OrderConverter;
import org.apache.dubbo.config.annotation.DubboService;
import javax.annotation.Resource;

@DubboService
@ExceptionAnnotation
public class OrderProviderImpl implements OrderProvider {

    @Resource
    private CommandBus commandBus;
    @Resource
    private QueryBus queryBus;

    @Override
    public Result<OrderResp> createOrder(CreateOrderReq req) {
        CreateOrderCommand cmd = new CreateOrderCommand();
        cmd.setUserId(req.getUserId());
        cmd.setChannel(req.getChannel());
        cmd.setAmount(req.getAmount());
        Order order = commandBus.dispatch(cmd);
        return Result.success(OrderConverter.toResp(order));
    }

    @Override
    public Result<OrderResp> queryOrder(QueryOrderReq req) {
        QueryOrderCommand qry = new QueryOrderCommand();
        qry.setOrderId(req.getOrderId());
        Order order = queryBus.dispatch(qry);
        return Result.success(OrderConverter.toResp(order));
    }
}
