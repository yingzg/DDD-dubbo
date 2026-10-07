package com.ddd.dubbo.domain.common.exception;

/** 系统异常：不可预期、技术故障，msg 模糊化、要告警 */
public class SysException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public SysException(int code, String msg, Throwable cause) {
        super(msg, cause);
        this.code = code;
    }

    public int getCode() { return code; }
}
