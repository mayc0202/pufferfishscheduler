package com.pufferfishscheduler.plugin.common.domain;

import java.util.List;
import java.util.Map;

/**
 * 表元数据
 *
 * @author Mayc
 * @since 2026-02-24  16:26
 */
public class TableMetadata {
    private String tableName;
    private String tableComment;
    private List<ColumnMetadata> columns;
    private Map<String, Object> sampleData;
    private Integer rowCount; // 可选的：总行数

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetadata> columns) {
        this.columns = columns;
    }

    public Map<String, Object> getSampleData() {
        return sampleData;
    }

    public void setSampleData(Map<String, Object> sampleData) {
        this.sampleData = sampleData;
    }

    public Integer getRowCount() {
        return rowCount;
    }

    public void setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
    }
}
