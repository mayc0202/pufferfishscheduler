package com.pufferfishscheduler.domain.domain;

import lombok.Data;

import java.util.Map;

/**
 * 表结构定义
 * @author Mayc
 * @since 2025-08-14  23:35
 */
@Data
public class TableSchema {

    /**
     * 表名称
     */
    private String tableName;
    /**
     * 表描述
     */
    private String tableComment;
    /**
     * 表类型
     */
    private String tableType;
    /**
     * 表sql
     */
    private String tableSql;

    /**
     * 缓存字典信息
     */
    private Map<String, TableColumnSchema> columnInfos;

}
