package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/** 命令分发总线：按泛型自动路由到匹配的 CmdExe */
@Component
public class CommandBus {

    private static final int SYS_ERROR = 5000;

    private final Map<Class<?>, CmdExe> handlers;

    @SuppressWarnings("unchecked")
    public CommandBus(Map<String, CmdExe> cmdExes) {
        this.handlers = new HashMap<Class<?>, CmdExe>();
        for (CmdExe exe : cmdExes.values()) {
            Class<?> targetClass = org.springframework.util.ClassUtils.getUserClass(exe.getClass());
            Class<?> cmdType = org.springframework.core.ResolvableType
                    .forClass(targetClass).as(CmdExe.class).getGeneric(0).resolve();
            if (cmdType != null) {
                handlers.put(cmdType, exe);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Command<R>, R> R dispatch(T command) {
        CmdExe exe = handlers.get(command.getClass());
        if (exe == null) {
            throw new BizException(
                    SYS_ERROR, "未找到命令执行器: " + command.getClass().getName());
        }
        return (R) exe.execute(command);
    }
}
