package com.ddd.dubbo.domain.extension;

import java.lang.annotation.*;

/** 扩展实现注解：声明该实现适用的业务场景 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Extension {
    String bizId();
    String useCase() default "default";
    String scenario() default "default";
}
