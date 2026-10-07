package com.ddd.dubbo.domain.common.exception;

/** 通用错误码（参数/系统段） */
public enum ErrorCodeEnums implements ErrorCode {
    PARAM_IS_EMPTY(1001, "参数不能为空"),
    PARAM_IS_INVALID(1002, "参数非法"),
    SYS_ERROR(5000, "系统异常，请稍后重试");

    private final int code;
    private final String msg;

    ErrorCodeEnums(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public int getCode() { return code; }

    @Override
    public String getMsg() { return msg; }
}
