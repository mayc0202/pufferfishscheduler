package com.pufferfishscheduler.common.condition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 条件树
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConditionTree {
    // 条件
    private String condition;

    // 存储where分组
    private List<ConditionGroup> conditionGroups;
}
