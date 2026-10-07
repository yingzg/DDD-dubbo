package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Map;

/** 命令分发总线：按泛型自动路由到匹配的 CmdExe */
@Component
public class CommandBus {

    private static final int SYS_ERROR = 5000;

    private final Map<String, CmdExe> cmdExes;

    public CommandBus(Map<String, CmdExe> cmdExes) {
        this.cmdExes = cmdExes;
    }

    @SuppressWarnings("unchecked")
    public <T extends Command<R>, R> R dispatch(T command) {
        for (CmdExe exe : cmdExes.values()) {
            Class<?> cmdType = ResolvableType.forInstance(exe)
                    .as(CmdExe.class).getGeneric(0).resolve();
            if (cmdType != null && cmdType.isInstance(command)) {
                return (R) exe.execute(command);
            }
        }
        throw new BizException(SYS_ERROR, "未找到命令执行器: " + command.getClass().getName());
    }
}
