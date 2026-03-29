package com.pufferfishscheduler.plugin.common.condition;


/**
 * 条件参数
 */
public class ConditionParam {

    // 列名称
    private String columnName;

    // 列数据类型
    private String dataType;

    // 过滤条件
    private String filterCondition;

    // 过滤值
    private Object filterValue;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public Object getFilterValue() {
        return filterValue;
    }

    public void setFilterValue(Object filterValue) {
        this.filterValue = filterValue;
    }
}
