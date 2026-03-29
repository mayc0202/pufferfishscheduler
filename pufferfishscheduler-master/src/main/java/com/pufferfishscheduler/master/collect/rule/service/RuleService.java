package com.pufferfishscheduler.master.collect.rule.service;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.vo.collect.RuleInformationVo;
import com.pufferfishscheduler.domain.form.collect.RuleForm;
import com.pufferfishscheduler.domain.vo.collect.RuleDetailVo;
import com.pufferfishscheduler.domain.vo.collect.RuleVo;

import java.util.List;
import java.util.Map;

/**
 * 规则服务接口
 *
 * @author Mayc
 * @since 2026-03-18  15:55
 */
public interface RuleService {

    /**
     * 分页查询规则
     *
     * @param groupId  分组ID
     * @param ruleName 规则名称
     * @param pageNo   页码
     * @param pageSize 每页数量
     * @return 规则分页列表VO对象¬
     */
    IPage<RuleVo> list(Integer groupId, String ruleName, Integer pageNo, Integer pageSize);

    /**
     * 规则详情
     *
     * @param id 规则ID
     * @return 规则详情VO对象
     */
    RuleDetailVo detail(String id);

    /**
     * 新增规则
     *
     * @param form 规则表单
     */
    void add(RuleForm form);

    /**
     * 编辑规则
     *
     * @param form 规则表单
     */
    void update(RuleForm form);

    /**
     * 逻辑删除规则
     *
     * @param id 规则ID
     */
    void delete(String id);

    /**
     * 发布/禁用规则
     *
     * @param id     规则ID
     * @param status 发布/禁用状态
     */
    void release(String id, Boolean status);

    /**
     * 分组+规则树（只包含已发布规则）
     *
     * @return 分组+规则树列表
     */
    List<Tree> tree();

    /**
     * 校验Java自定义规则脚本语法
     *
     * @param json Java脚本json对象
     */
    void validateJavaCode(JSONObject json);

    /**
     * 预览Java自定义规则结果
     *
     * @param json 预览数据json对象
     * @return 预览数据map
     */
    Object previewJavaCode(JSONObject json);

    /**
     * 校验sql脚本语法
     *
     * @param json sql脚本json对象
     */
    void validateSql(JSONObject json);

    /**
     * 预览数据
     *
     * @param json 预览数据json对象
     * @return 预览数据map
     */
    Map<String, Object> preview(JSONObject json);

    /**
     * 获取规则配置(已发布)
     *
     * @param ruleIds 规则ID列表
     * @return 规则配置列表
     */
    List<RuleInformationVo> getRuleInformation(JSONArray ruleIds);
}
