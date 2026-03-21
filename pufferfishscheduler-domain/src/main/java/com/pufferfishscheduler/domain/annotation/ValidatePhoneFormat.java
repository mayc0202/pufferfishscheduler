package com.pufferfishscheduler.domain.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 校验手机号码格式
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidatePhoneFormat {

    /**
     * 是否允许为空
     *
     * @return 是否允许为空
     */
    boolean allowEmpty() default false;

    /**
     * 校验失败时的错误提示信息
     *
     * @return 错误提示
     */
    String message() default "手机号码格式不正确!";
}
