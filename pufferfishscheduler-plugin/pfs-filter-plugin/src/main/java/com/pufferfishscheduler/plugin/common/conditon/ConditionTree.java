package com.pufferfishscheduler.plugin.common.conditon;

import java.util.List;

/**
 * 条件树
 */
public class ConditionTree {
    // 条件
    private String condition;

    // 存储where分组
    private List<ConditionGroup> conditionGroups;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<ConditionGroup> getConditionGroups() {
        return conditionGroups;
    }

    public void setConditionGroups(List<ConditionGroup> conditionGroups) {
        this.conditionGroups = conditionGroups;
    }
}
