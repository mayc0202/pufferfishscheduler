package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 规则树节点（分组+规则）
 */
@Data
public class RuleTreeVo {
    private String id;
    private String label;
    private String type;
    private String parentId;
    private List<RuleTreeVo> children = new ArrayList<>();
}
