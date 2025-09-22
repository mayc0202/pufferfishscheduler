package com.pufferfishscheduler.domain.domain;

import lombok.Data;

/**
 *
 * @author Mayc
 * @since 2025-08-15  00:09
 */
@Data
public class TableForeignKey {
    /*
     * 外键名称
     */
    private String name;

    /*
     * 表名称
     */
    private String tableName;

    /*
     * 字段名称，多个以英文逗号分隔
     */
    private String columnNames;

    /*
     * 引用表名称
     */
    private String refTableName;

    /*
     * 引用字段名称
     */
    private String refColumnNames;
}
