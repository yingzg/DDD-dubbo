package com.ddd.dubbo.domain.exe;

/** 查询执行器（读操作） */
public interface QryExe<Q extends Query<R>, R> {
    R execute(Q qry);
}
