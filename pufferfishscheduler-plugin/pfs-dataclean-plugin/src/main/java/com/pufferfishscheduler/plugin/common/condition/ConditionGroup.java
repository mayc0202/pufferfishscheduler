package com.pufferfishscheduler.plugin.common.condition;

import java.util.List;

/**
 * 条件组
 */
public class ConditionGroup {

    // 关系 or and
    private String condition;

    // 查询条件
    private List<ConditionParam> queryConditions;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<ConditionParam> getQueryConditions() {
        return queryConditions;
    }

    public void setQueryConditions(List<ConditionParam> queryConditions) {
        this.queryConditions = queryConditions;
    }
}
