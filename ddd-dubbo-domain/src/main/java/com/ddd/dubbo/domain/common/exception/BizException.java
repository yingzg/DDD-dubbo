package com.ddd.dubbo.domain.common.exception;

/** 业务异常：可预期、对调用方可见、带业务错误码，不告警 */
public class BizException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public BizException(ErrorCode errorCode) {
        super(errorCode.getMsg());
        this.code = errorCode.getCode();
    }

    public BizException(int code, String msg) {
        super(msg);
        this.code = code;
    }

    public int getCode() { return code; }
}
