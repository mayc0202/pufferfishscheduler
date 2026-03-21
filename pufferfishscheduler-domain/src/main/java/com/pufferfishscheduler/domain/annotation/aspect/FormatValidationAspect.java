package com.pufferfishscheduler.domain.annotation.aspect;

import com.pufferfishscheduler.domain.annotation.ValidateEmailFormat;
import com.pufferfishscheduler.domain.annotation.ValidatePhoneFormat;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.regex.Pattern;

/**
 * 格验校验切面
 *
 * @author Mayc
 * @since 2026-03-21  13:56
 */
@Aspect
@Component
public class FormatValidationAspect {

    // 手机号码正则表达式（支持11位手机号）
    private static final String PHONE_REGEX = "^1[3-9]\\d{9}$";
    private static final Pattern PHONE_PATTERN = Pattern.compile(PHONE_REGEX);
    // 邮箱正则表达式
    private static final String EMAIL_REGEX = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$";
    private static final Pattern EMAIL_PATTERN = Pattern.compile(EMAIL_REGEX);

    /**
     * 拦截所有Controller方法，校验带有注解的字段
     */
    @Before("execution(* com.pufferfishscheduler..*(..))")
    public void validateFields(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();

        for (Object arg : args) {
            if (arg == null) {
                continue;
            }
            // 校验对象的所有字段
            validateObjectFields(arg);
        }
    }

    /**
     * 校验对象的所有字段
     */
    private void validateObjectFields(Object obj) {
        Class<?> clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            // 校验手机号码字段
            if (field.isAnnotationPresent(ValidatePhoneFormat.class)) {
                validatePhoneField(obj, field);
            }

            // 校验邮箱字段
            if (field.isAnnotationPresent(ValidateEmailFormat.class)) {
                validateEmailField(obj, field);
            }
        }
    }

    /**
     * 校验邮箱字段
     */
    private void validateEmailField(Object obj, Field field) {
        try {
            ValidateEmailFormat annotation = field.getAnnotation(ValidateEmailFormat.class);
            String value = (String) field.get(obj);

            // 空值处理
            if (!StringUtils.hasText(value)) {
                if (!annotation.allowEmpty()) {
                    throw new RuntimeException("邮箱不能为空!");
                }
                return;
            }

            String trimmedValue = value.trim();

            // 长度校验
            if (trimmedValue.length() > 64) {
                throw new RuntimeException("邮箱长度不能超过64!");
            }

            // 格式校验
            if (!EMAIL_PATTERN.matcher(trimmedValue).matches()) {
                throw new RuntimeException(annotation.message());
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("邮箱访问异常：" + e.getMessage());
        }
    }

    /**
     * 校验手机号码字段
     */
    private void validatePhoneField(Object obj, Field field) {
        try {
            ValidatePhoneFormat annotation = field.getAnnotation(ValidatePhoneFormat.class);
            String value = (String) field.get(obj);

            // 空值处理
            if (!StringUtils.hasText(value)) {
                if (!annotation.allowEmpty()) {
                    throw new IllegalStateException("手机号码不能为空!");
                }
                return;
            }

            String trimmedValue = value.trim();

            // 长度校验
            if (trimmedValue.length() != 11) {
                throw new IllegalStateException("手机号码必须是11位数字");
            }

            // 格式校验
            if (!PHONE_PATTERN.matcher(trimmedValue).matches()) {
                throw new IllegalStateException(annotation.message());
            }

        } catch (IllegalAccessException e) {
            throw new IllegalStateException("手机号码访问异常：" + e.getMessage());
        }
    }
}
