package com.pufferfishscheduler.domain.vo.collect;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 规则分类菜单树节点
 */
@Data
public class RuleGroupMenuVo {

    private Integer id;

    private String label;

    private Integer parentId;

    /**
     * 顶层分类ID（公共/自定义等）
     */
    private Integer firstLayerId;

    private List<RuleGroupMenuVo> children = new ArrayList<>();
}
