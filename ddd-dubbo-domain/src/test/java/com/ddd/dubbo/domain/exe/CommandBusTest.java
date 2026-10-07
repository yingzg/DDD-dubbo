package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandBusTest {

    static class CreateCmd implements Command<String> {}
    static class UpdateCmd implements Command<String> {}

    static class CreateExe implements CmdExe<CreateCmd, String> {
        @Override
        public String execute(CreateCmd cmd) { return "created"; }
    }

    static class UpdateExe implements CmdExe<UpdateCmd, String> {
        @Override
        public String execute(UpdateCmd cmd) { return "updated"; }
    }

    @Test
    void routesToMatchingExecutorByGenericType() {
        CmdExe<CreateCmd, String> createExe = new CmdExe<CreateCmd, String>() {
            @Override
            public String execute(CreateCmd cmd) { return "created"; }
        };
        CmdExe<UpdateCmd, String> updateExe = new CmdExe<UpdateCmd, String>() {
            @Override
            public String execute(UpdateCmd cmd) { return "updated"; }
        };
        Map<String, CmdExe> map = new HashMap<String, CmdExe>();
        map.put("create", createExe);
        map.put("update", updateExe);

        CommandBus bus = new CommandBus(map);

        assertEquals("created", bus.dispatch(new CreateCmd()));
        assertEquals("updated", bus.dispatch(new UpdateCmd()));
    }

    @Test
    void routesPreciselyAcrossMultipleExecutors() {
        Map<String, CmdExe> map = new HashMap<String, CmdExe>();
        map.put("create", new CreateExe());
        map.put("update", new UpdateExe());

        CommandBus bus = new CommandBus(map);

        assertEquals("created", bus.dispatch(new CreateCmd()));
        assertEquals("updated", bus.dispatch(new UpdateCmd()));
        assertEquals("created", bus.dispatch(new CreateCmd()));
    }

    @Test
    void routesWhenExecutorIsCglibProxied() {
        ProxyFactory factory = new ProxyFactory(new CreateExe());
        factory.setProxyTargetClass(true);
        CmdExe<CreateCmd, String> proxy = (CmdExe<CreateCmd, String>) factory.getProxy();
        Map<String, CmdExe> map = new HashMap<String, CmdExe>();
        map.put("create", proxy);

        CommandBus bus = new CommandBus(map);

        assertEquals("created", bus.dispatch(new CreateCmd()));
    }

    @Test
    void throwsWhenNoExecutorMatched() {
        CommandBus bus = new CommandBus(new HashMap<String, CmdExe>());
        BizException e = assertThrows(BizException.class, () -> bus.dispatch(new CreateCmd()));
        assertTrue(e.getMessage().contains("未找到命令执行器"));
    }
}
