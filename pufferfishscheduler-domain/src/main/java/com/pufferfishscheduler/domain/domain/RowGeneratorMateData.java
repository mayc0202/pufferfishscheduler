package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RowGeneratorMateData {

    /**
     * 字段名称
     */
    private String fieldName;

    /**
     * 字段类型
     */
    private String typeText;

    /**
     * 字段格式
     */
    private String fieldFormat;

    /**
     * 字段长度
     */
    private int fieldLength;

    /**
     * 字段精度
     */
    private int fieldPrecision;

    /**
     * 字段小数符号
     */
    private String decimal;

    /**
     * 字段值
     */
    private String value;

    /**
     * 字段千分位符号
     */
    private String group;
}
