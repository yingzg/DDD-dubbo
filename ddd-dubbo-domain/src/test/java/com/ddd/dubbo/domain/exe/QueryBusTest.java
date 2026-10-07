package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryBusTest {

    static class GetUserQry implements Query<String> {}
    static class GetOrderQry implements Query<String> {}

    static class GetUserExe implements QryExe<GetUserQry, String> {
        @Override
        public String execute(GetUserQry qry) { return "user"; }
    }

    static class GetOrderExe implements QryExe<GetOrderQry, String> {
        @Override
        public String execute(GetOrderQry qry) { return "order"; }
    }

    @Test
    void routesPreciselyAcrossMultipleExecutors() {
        Map<String, QryExe> map = new HashMap<String, QryExe>();
        map.put("user", new GetUserExe());
        map.put("order", new GetOrderExe());

        QueryBus bus = new QueryBus(map);

        assertEquals("user", bus.dispatch(new GetUserQry()));
        assertEquals("order", bus.dispatch(new GetOrderQry()));
        assertEquals("user", bus.dispatch(new GetUserQry()));
    }

    @Test
    void routesWhenExecutorIsCglibProxied() {
        ProxyFactory factory = new ProxyFactory(new GetUserExe());
        factory.setProxyTargetClass(true);
        QryExe<GetUserQry, String> proxy = (QryExe<GetUserQry, String>) factory.getProxy();
        Map<String, QryExe> map = new HashMap<String, QryExe>();
        map.put("user", proxy);

        QueryBus bus = new QueryBus(map);

        assertEquals("user", bus.dispatch(new GetUserQry()));
    }

    @Test
    void throwsWhenNoExecutorMatched() {
        QueryBus bus = new QueryBus(new HashMap<String, QryExe>());
        BizException e = assertThrows(BizException.class, () -> bus.dispatch(new GetUserQry()));
        assertTrue(e.getMessage().contains("未找到查询执行器"));
    }
}
