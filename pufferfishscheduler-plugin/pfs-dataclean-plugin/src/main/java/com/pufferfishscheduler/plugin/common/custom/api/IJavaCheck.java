package com.pufferfishscheduler.plugin.common.custom.api;

import com.pufferfishscheduler.plugin.common.custom.Param;

public interface IJavaCheck {

    /**
     * 转换
     * @param row 存储参数
     * @param value 需要转换的值
     * @return
     */
    Object convert(Param row, Object value);
}
