package com.ddd.dubbo.domain.common.exception;

/** 错误码接口：错误码分段 0 成功 / 1xxx 参数 / 2xxx 业务 / 5xxx 系统 */
public interface ErrorCode {
    int getCode();
    String getMsg();
}
