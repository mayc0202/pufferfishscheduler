package com.pufferfishscheduler.domain.form.collect;

import lombok.Data;

/**
 * 设置变量表单
 */
@Data
public class SetVariableForm {

    //字段名称
    private String fieldName;

    //变量
    private String variableName;

    //变量有效范围
    private int variableType;
}
