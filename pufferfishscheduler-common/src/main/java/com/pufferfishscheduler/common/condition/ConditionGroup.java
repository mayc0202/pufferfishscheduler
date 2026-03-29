package com.pufferfishscheduler.common.condition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 条件组
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConditionGroup {

    // 关系 or and
    private String condition;

    // 查询条件
    private List<ConditionParam> queryConditions;

}
