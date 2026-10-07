package com.ddd.dubbo.domain.exe;

/** 命令执行器（写操作） */
public interface CmdExe<C extends Command<R>, R> {
    R execute(C cmd);
}
