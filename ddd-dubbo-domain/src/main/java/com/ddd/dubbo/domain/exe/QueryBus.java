package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/** 查询分发总线：按泛型自动路由到匹配的 QryExe */
@Component
public class QueryBus {

    private static final int SYS_ERROR = 5000;

    private final Map<Class<?>, QryExe> handlers;

    @SuppressWarnings("unchecked")
    public QueryBus(Map<String, QryExe> qryExes) {
        this.handlers = new HashMap<Class<?>, QryExe>();
        for (QryExe exe : qryExes.values()) {
            Class<?> targetClass = org.springframework.util.ClassUtils.getUserClass(exe.getClass());
            Class<?> qryType = org.springframework.core.ResolvableType
                    .forClass(targetClass).as(QryExe.class).getGeneric(0).resolve();
            if (qryType != null) {
                handlers.put(qryType, exe);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<R>, R> R dispatch(T query) {
        QryExe exe = handlers.get(query.getClass());
        if (exe == null) {
            throw new BizException(
                    SYS_ERROR, "未找到查询执行器: " + query.getClass().getName());
        }
        return (R) exe.execute(query);
    }
}
