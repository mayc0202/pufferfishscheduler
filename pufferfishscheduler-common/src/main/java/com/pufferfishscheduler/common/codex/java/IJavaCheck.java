package com.pufferfishscheduler.common.codex.java;

/**
 * Java自定义规则接口
 */
public interface IJavaCheck {

    /**
     * 转换
     * @param param 存储参数
     * @param value 需要转换的值
     * @return 转换后的值
     */
    Object convert(Param param, Object value);
}

