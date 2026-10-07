package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Map;

/** 查询分发总线：按泛型自动路由到匹配的 QryExe */
@Component
public class QueryBus {

    private static final int SYS_ERROR = 5000;

    private final Map<String, QryExe> qryExes;

    public QueryBus(Map<String, QryExe> qryExes) {
        this.qryExes = qryExes;
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<R>, R> R dispatch(T query) {
        for (QryExe exe : qryExes.values()) {
            Class<?> qryType = ResolvableType.forInstance(exe)
                    .as(QryExe.class).getGeneric(0).resolve();
            if (qryType != null && qryType.isInstance(query)) {
                return (R) exe.execute(query);
            }
        }
        throw new BizException(SYS_ERROR, "未找到查询执行器: " + query.getClass().getName());
    }
}
