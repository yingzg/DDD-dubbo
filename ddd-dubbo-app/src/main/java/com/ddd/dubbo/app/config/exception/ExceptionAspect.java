package com.ddd.dubbo.app.config.exception;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.domain.common.context.TraceContext;
import com.ddd.dubbo.domain.common.exception.BizException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 统一异常切面：只拦「@DubboService + @ExceptionAnnotation」的类 */
@Aspect
@Component
public class ExceptionAspect {

    private static final Logger log = LoggerFactory.getLogger(ExceptionAspect.class);
    private static final int SYS_ERROR_CODE = 5000;

    @Around("@within(com.ddd.dubbo.app.config.exception.ExceptionAnnotation) " +
            "&& @within(org.apache.dubbo.config.annotation.DubboService)")
    public Object handle(ProceedingJoinPoint pjp) {
        try {
            return pjp.proceed();
        } catch (BizException e) {
            log.info("[biz] method={} code={} msg={}", pjp.getSignature(), e.getCode(), e.getMessage());
            return Result.fail(e.getCode(), e.getMessage()).withTraceId(TraceContext.getTraceId());
        } catch (Throwable t) {
            log.error("[sys] method={}", pjp.getSignature(), t);
            return Result.fail(SYS_ERROR_CODE, "系统异常，请稍后重试").withTraceId(TraceContext.getTraceId());
        }
    }
}
