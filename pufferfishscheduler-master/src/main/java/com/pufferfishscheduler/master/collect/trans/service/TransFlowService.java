package com.pufferfishscheduler.master.collect.trans.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;

import java.util.List;

/**
 * 转换流服务
 *
 * @author Mayc
 * @since 2026-03-04  17:07
 */
public interface TransFlowService {

    /**
     * 分页查询
     *
     * @param groupId
     * @param flowName
     * @param pageNo
     * @param pageSize
     * @return
     */
    IPage<TransFlowVo> list(Integer groupId, String flowName, Integer pageNo, Integer pageSize);

    /**
     * 转换分组（父子）+ 转换流程 树形结构
     *
     * @param name 分组名称模糊筛选（可选，与分组列表接口一致）
     * @return 树节点列表
     */
    List<Tree> tree(String name);

    /**
     * 添加转换流
     *
     * @param flow  转换流表单
     * @param stage 数据处理阶段
     * @return 转换流id
     */
    Integer addFlow(TransFlowForm flow, String stage);

    /**
     * 更新转换流
     *
     * @param flow 转换流表单
     * @return
     */
    void updateFlow(TransFlowForm flow);

    /**
     * 删除转换流
     *
     * @param id 转换流id
     * @return
     */
    void deleteFlow(Integer id);

    /**
     * 设置转换流配置
     *
     * @param flow 转换流配置表单
     * @return
     */
    void setConfig(TransFlowConfigForm flow);

    /**
     * 查询转换流详情
     *
     * @param id 转换流id
     * @return
     */
    TransFlowVo detail(Integer id);

    /**
     * 执行转换流
     *
     * @param id 转换流id
     * @return
     */
    void execute(Integer id);

    /**
     * 停止转换流
     *
     * @param id 转换流id
     * @return
     */
    void stop(Integer id);

    /**
     * 复制转换流
     *
     * @param id 转换流id
     * @return
     */
    void copy(Integer id);
}
