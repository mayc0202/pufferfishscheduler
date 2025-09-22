package com.pufferfishscheduler.common.result;

import lombok.*;

/**
 * API接口统一响应结果封装类
 *
 * @author Mayc
 * @since 2025-09-21  00:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApiResponse {
    // 响应状态码常量
    public static final String SUCCESS_CODE = "000000";
    public static final String DEFAULT_ERROR_CODE = "999999";

    // 响应状态描述常量
    public static final String SUCCESS_STATUS = "OK";
    public static final String ERROR_STATUS = "ERROR";

    // 响应数据字段
    private String code;         // 响应状态码
    private String message;      // 响应消息
    private Object data;         // 响应数据主体
    private String status;       // 响应状态描述

    /**
     * 构建成功地响应（无数据）
     */
    public static ApiResponse success() {
        return success(null);
    }

    /**
     * 构建成功的响应（带数据）
     */
    public static ApiResponse success(Object data) {
        ApiResponse response = new ApiResponse();
        response.setCode(SUCCESS_CODE);
        response.setStatus(SUCCESS_STATUS);
        response.setData(data);
        return response;
    }

    /**
     * 构建失败的响应（使用默认错误码）
     */
    public static ApiResponse failure(String message) {
        return failure(DEFAULT_ERROR_CODE, message);
    }

    /**
     * 构建失败的响应（指定错误码）
     */
    public static ApiResponse failure(String code, String message) {
        return failure(null, code, message);
    }

    /**
     * 构建失败的响应（带附加数据和错误码）
     */
    public static ApiResponse failure(Object data, String code, String message) {
        ApiResponse response = new ApiResponse();
        response.setCode(code);
        response.setMessage(message);
        response.setStatus(ERROR_STATUS);
        response.setData(data);
        return response;
    }

}
