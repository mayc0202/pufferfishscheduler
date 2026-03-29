package com.pufferfishscheduler.common.condition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 条件参数
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConditionParam {

    // 列名称
    private String columnName;

    // 列数据类型
    private String dataType;

    // 过滤条件
    private String filterCondition;

    // 过滤值
    private Object filterValue;
}
