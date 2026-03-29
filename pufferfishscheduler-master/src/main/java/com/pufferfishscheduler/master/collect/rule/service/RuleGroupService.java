package com.pufferfishscheduler.master.collect.rule.service;

import com.pufferfishscheduler.domain.form.collect.RuleGroupForm;
import com.pufferfishscheduler.common.node.Tree;

import java.util.List;

/**
 * 规则组服务接口
 * @author Mayc
 * @since 2026-03-18  15:54
 */
public interface RuleGroupService {

    /**
     * 获取规则分类树
     */
    List<Tree> tree(String name);

    /**
     * 新增分类
     */
    void add(RuleGroupForm form);

    /**
     * 编辑分类
     */
    void update(RuleGroupForm form);

    /**
     * 删除分类
     */
    void delete(Integer id);

    /**
     * 固定分类ID集合
     */
    String[] getRegularGroupId();
}
