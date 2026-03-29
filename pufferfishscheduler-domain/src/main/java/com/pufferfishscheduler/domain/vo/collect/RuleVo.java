package com.pufferfishscheduler.domain.vo.collect;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.Date;

/**
 * 规则列表VO
 */
@Data
public class RuleVo {

    private String id;
    private Integer groupId;
    private String groupName;
    private Integer firstGroupId;
    private Integer ruleType;
    private String ruleTypeTxt;
    private String ruleCode;
    private String ruleName;
    private String ruleDescription;
    private Integer ruleProcessorId;
    private String processorName;
    private Boolean status;
    private String statusTxt;
    private Boolean deleted;
    private String createdBy;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createdTime;
    private String createdTimeTxt;
    private String updatedBy;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date updatedTime;
    private String updatedTimeTxt;
}
