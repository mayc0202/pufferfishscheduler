package com.pufferfishscheduler.common.exception;

/**
 * @Author: yc
 * @CreateTime: 2025-05-17
 * @Description:
 * @Version: 1.0
 */
public class BusinessException extends RuntimeException {

    private String errorCode;
    private String message;

    public BusinessException() {
    }

    public BusinessException(Throwable e) {
        super(e);
    }

    public BusinessException(String message) {
        this.message = message;
    }

    public BusinessException(String errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public String getErrorCode() {
        return this.errorCode;
    }

    public String getMessage() {
        return this.message;
    }
}