package com.pufferfishscheduler.domain.domain;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据清洗规则元数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RuleMetaData {
    private Integer groupId;
    private String ruleId;
    private String ruleName;
    private String ruleCode;
    private String ruleDescription;
    private Integer ruleProcessorId;
    private String sqlCode;
    private String tableName;
    private Integer dataSourceId;
    private String afterfieldName;
    private String beforefieldName;
    private JSONObject outerQueryParams;
    private JSONArray fieldList;
    private String javaCode;
    private String renameType;
    private String rename;
    private String mappingType;
}
