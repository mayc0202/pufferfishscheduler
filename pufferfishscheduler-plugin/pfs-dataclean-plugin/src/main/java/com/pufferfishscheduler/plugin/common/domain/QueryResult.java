package com.pufferfishscheduler.plugin.common.domain;

import java.util.List;
import java.util.Map;

/**
 * 查询结果
 *
 * @author Mayc
 * @since 2026-02-24  16:28
 */
public class QueryResult {
    private String sql;
    private List<Map<String, Object>> data;
    private int rowCount;
    private long executionTime;
    private String errorMessage;
    private boolean success;
    private Map<String, Object> summary; // 可选的：统计信息

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Map<String, Object> getSummary() {
        return summary;
    }

    public void setSummary(Map<String, Object> summary) {
        this.summary = summary;
    }
}
