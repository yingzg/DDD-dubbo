package com.ddd.dubbo.job.task;

import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.context.XxlJobHelper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** XXL-Job 任务基类：手动建 OTel root span，traceId 写 MDC */
public abstract class AbstractTask {

    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("ddd-dubbo-job");

    @XxlJob("taskHandler")
    public void execute() {
        String taskName = getClass().getSimpleName();
        Span span = TRACER.spanBuilder("job." + taskName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("job.name", taskName)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            MDC.put("traceId", span.getSpanContext().getTraceId());
            doExecute();
            XxlJobHelper.handleSuccess();
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            log.error("任务执行失败", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            XxlJobHelper.handleFail(e.getMessage());
        } finally {
            MDC.remove("traceId");
            span.end();
        }
    }

    protected abstract void doExecute();
}
