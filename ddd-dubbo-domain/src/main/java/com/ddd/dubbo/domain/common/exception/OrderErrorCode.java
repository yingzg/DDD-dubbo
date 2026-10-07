package com.ddd.dubbo.domain.common.exception;

/** 订单域业务错误码（2xxx 业务段） */
public enum OrderErrorCode implements ErrorCode {
    ORDER_NOT_FOUND(2001, "订单不存在"),
    STOCK_NOT_ENOUGH(2002, "库存不足"),
    ORDER_STATUS_ERROR(2003, "订单状态不允许此操作");

    private final int code;
    private final String msg;

    OrderErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public int getCode() { return code; }

    @Override
    public String getMsg() { return msg; }
}
