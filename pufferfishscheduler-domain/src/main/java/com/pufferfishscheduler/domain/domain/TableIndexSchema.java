package com.pufferfishscheduler.domain.domain;

import lombok.Data;

/**
 *
 * @author Mayc
 * @since 2025-08-15  00:22
 */
@Data
public class TableIndexSchema {

    /*
     * 索引名称
     */
    private String name;

    /*
     * 表名称
     */
    private String tableName;

    /*
     * 是否唯一索引
     */
    private boolean unique;

    /*
     * 字段名称，多个以英文逗号分隔
     */
    private String columnNames;

    /**
     * 是否是主键索引
     */
    private boolean primaryKey;

    /**
     * 索引类型
     */
    private String type;

    /**
     * 索引扩展属性
     */
    private String properties;

    /**
     * 索引描述
     */
    private String description;

}
