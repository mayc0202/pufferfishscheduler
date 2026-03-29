package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

/**
 * 规则分类详情
 */
@Data
public class RuleGroupVo {

    private Integer id;

    private String groupName;

    private Integer groupType;

    private Integer parentId;

    private String createdTime;

    private String createdBy;

    private String updatedBy;

    private String updatedTime;

    private Integer orderBy;
}
