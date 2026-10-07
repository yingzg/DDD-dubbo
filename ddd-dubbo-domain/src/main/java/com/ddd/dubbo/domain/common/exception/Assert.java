package com.ddd.dubbo.domain.common.exception;

import java.util.Collection;
import java.util.Map;

/** 断言工具（借鉴 COLA Assert 风格，错误码用 ErrorCode 枚举） */
public final class Assert {

    private Assert() {
    }

    public static void isTrue(boolean expression, ErrorCode errorCode) {
        if (!expression) {
            throw new BizException(errorCode);
        }
    }

    public static void isFalse(boolean expression, ErrorCode errorCode) {
        if (expression) {
            throw new BizException(errorCode);
        }
    }

    public static void notNull(Object object, ErrorCode errorCode) {
        if (object == null) {
            throw new BizException(errorCode);
        }
    }

    public static void notEmpty(Collection<?> collection, ErrorCode errorCode) {
        if (collection == null || collection.isEmpty()) {
            throw new BizException(errorCode);
        }
    }

    public static void notEmpty(Map<?, ?> map, ErrorCode errorCode) {
        if (map == null || map.isEmpty()) {
            throw new BizException(errorCode);
        }
    }
}
