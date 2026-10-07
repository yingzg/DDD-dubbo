package com.ddd.dubbo.domain.common.exception;

import java.lang.annotation.*;

/** 开启统一异常切面的标记注解 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ExceptionAnnotation {
}
