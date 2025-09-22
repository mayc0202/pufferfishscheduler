package com.pufferfishscheduler.domain.domain;

import lombok.Data;

/**
 * 列结构定义
 *
 * @author Mayc
 * @since 2025-08-14  23:36
 */
@Data
public class TableColumnSchema {

    private String tableName;
    /**
     * 是否有键
     */
    private Boolean columnKey;
    /**
     * 列名称
     */
    private String columnName;
    /**
     * 列描述
     */
    private String columnComment;
    /**
     * 数据类型
     */
    private String dataType;
    /**
     * 是否为空
     */
    private Boolean isNull;
    /**
     * 约束类型
     */
    private String constraintType;
    /**
     * 数据长度
     */
    private Integer dataLength;
    /**
     * 精度
     */
    private Integer precision;
    /**
     * 排序
     */
    private Integer columnOrder;
}
