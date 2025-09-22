package com.pufferfishscheduler.common.bean;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * 从 Spring 上下文中获取 Bean 的工具类
 *
 * @author Mayc
 * @description
 * @since 2025-07-30  13:19
 */
@Component
public class EtlApplicationContext implements ApplicationContextAware {

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        EtlApplicationContext.context = ctx;
    }

    public static <T> T getBean(Class<T> requiredType) {
        return context.getBean(requiredType);
    }

    public static Object getBean(String name) {
        return context.getBean(name);
    }
}