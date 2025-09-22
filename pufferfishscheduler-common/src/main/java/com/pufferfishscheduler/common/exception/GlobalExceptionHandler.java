package com.pufferfishscheduler.common.exception;

import com.pufferfishscheduler.common.result.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;

import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;


/**
 * 全局异常处理器，统一处理应用中所有未捕获的异常
 *
 * @author mayc
 * @since 2025-05-22 00:00:40
 */
@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {


    // 系统默认错误提示
    private static final String SYSTEM_ERROR_MSG = "系统异常，请稍后再试！";
    // 参数校验错误前缀
    private static final String VALIDATION_ERROR_PREFIX = "参数校验失败：";

    /**
     * 处理所有未被特定异常处理器捕获的异常
     */
    @ExceptionHandler(Throwable.class)
    @ResponseBody
    public ApiResponse handleUncaughtException(Throwable e, HttpServletResponse response) {
        logException(e);
        return ApiResponse.failure(SYSTEM_ERROR_MSG);
    }

    /**
     * 处理业务异常
     */
    @ExceptionHandler(BusinessException.class)
    @ResponseBody
    public ApiResponse handleBusinessException(BusinessException e) {
        log.warn("业务异常: {}", e.getMessage());
        return ApiResponse.failure(e.getMessage());
    }

    /**
     * 处理参数不合法异常
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseBody
    public ApiResponse handleIllegalArgumentException(IllegalArgumentException e) {
        log.warn("参数不合法异常: {}", e.getMessage());
        return ApiResponse.failure(e.getMessage());
    }

    /**
     * 处理参数绑定异常
     */
    @ExceptionHandler(BindException.class)
    @ResponseBody
    public ApiResponse handleBindException(BindException e) {
        logException(e);
        String errorMsg = buildValidationErrorMessage(e.getFieldErrors());
        return ApiResponse.failure(VALIDATION_ERROR_PREFIX + errorMsg);
    }

    /**
     * 处理方法参数校验异常
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public ApiResponse handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        logException(e);

        if (!e.getBindingResult().hasErrors()) {
            log.warn("方法参数校验异常中未发现错误信息");
            return ApiResponse.failure(VALIDATION_ERROR_PREFIX + "参数校验失败");
        }

        String errorMsg = buildValidationErrorMessage(e.getBindingResult().getFieldErrors());
        return ApiResponse.failure(VALIDATION_ERROR_PREFIX + errorMsg);
    }

    /**
     * 处理不支持的请求方法异常
     */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    @ResponseBody
    public ApiResponse handleHttpRequestMethodNotSupportedException(HttpRequestMethodNotSupportedException e) {
        logException(e);
        return ApiResponse.failure("不支持的请求方法: " + e.getMessage());
    }

    /**
     * 处理请求参数缺失异常
     */
    @ExceptionHandler({ServletRequestBindingException.class, MissingServletRequestPartException.class})
    @ResponseBody
    public ApiResponse handleMissingParameterException(Exception e) {
        logException(e);
        return ApiResponse.failure("请求参数缺失: " + e.getMessage());
    }

    /**
     * 处理请求体解析异常
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    @ResponseBody
    public ApiResponse handleHttpMessageNotReadableException(HttpMessageNotReadableException e) {
        logException(e);
        return ApiResponse.failure("请求体解析失败: " + e.getMessage());
    }

    /**
     * 处理不支持的编码异常
     */
    @ExceptionHandler(UnsupportedEncodingException.class)
    @ResponseBody
    public ApiResponse handleUnsupportedEncodingException(UnsupportedEncodingException e) {
        logException(e);
        return ApiResponse.failure("不支持的编码格式: " + e.getMessage());
    }

    /**
     * 处理空指针异常
     */
    @ExceptionHandler(NullPointerException.class)
    @ResponseBody
    public ApiResponse handleNullPointerException(NullPointerException e) {
        logException(e);
        return ApiResponse.failure("空指针异常: " + e.getMessage());
    }

    /**
     * 处理数据库异常
     */
    @ExceptionHandler(SQLException.class)
    @ResponseBody
    public ApiResponse handleSQLException(SQLException e) {
        logException(e);
        return ApiResponse.failure("数据库操作异常: " + e.getMessage());
    }

    /**
     * 处理网络超时异常
     */
    @ExceptionHandler(SocketTimeoutException.class)
    @ResponseBody
    public ApiResponse handleSocketTimeoutException(SocketTimeoutException e) {
        logException(e);
        return ApiResponse.failure("服务连接超时: " + e.getMessage());
    }

    /**
     * 处理不支持的媒体类型异常
     */
    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    @ResponseBody
    public ApiResponse handleHttpMediaTypeNotAcceptableException(HttpMediaTypeNotAcceptableException e) {
        logException(e);
        return ApiResponse.failure("不支持的媒体类型: " + e.getMessage());
    }

    /**
     * 处理类型转换异常
     */
    @ExceptionHandler({NumberFormatException.class, MethodArgumentTypeMismatchException.class})
    @ResponseBody
    public ApiResponse handleTypeConversionException(Exception e) {
        logException(e);
        return ApiResponse.failure("类型转换异常: " + e.getMessage());
    }

    /**
     * 处理安全异常
     */
    @ExceptionHandler(AssertionError.class)
    @ResponseBody
    public ApiResponse handleSecurityException(AssertionError e) {
        logException(e);
        return ApiResponse.failure("加密解密异常：" + e.getMessage());
    }

    /**
     * 处理解析异常
     */
    @ExceptionHandler(ParseException.class)
    @ResponseBody
    public ApiResponse handleParseException(ParseException e) {
        logException(e);
        return ApiResponse.failure("解析异常：" + e.getMessage());
    }

    /**
     * 构建参数校验错误信息
     */
    private String buildValidationErrorMessage(List<FieldError> fieldErrors) {
        return fieldErrors.stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
    }

    /**
     * 记录异常日志，根据异常类型使用不同级别
     */
    private void logException(Throwable e) {
        if (e instanceof BusinessException) {
            log.warn("业务异常: {}", e.getMessage());
        } else if (e instanceof BindException || e instanceof MethodArgumentNotValidException) {
            log.warn("参数校验异常: {}", e.getMessage());
        } else {
            log.error("系统异常发生", e);
        }
    }
}