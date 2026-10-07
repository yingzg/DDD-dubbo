package com.ddd.dubbo.domain.common.context;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

/** traceId 上下文：优先读 OpenTelemetry 当前 Span */
public final class TraceContext {

    private TraceContext() {
    }

    public static String getTraceId() {
        SpanContext ctx = Span.current().getSpanContext();
        return ctx != null && ctx.isValid() ? ctx.getTraceId() : null;
    }
}
