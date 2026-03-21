package com.pufferfishscheduler.master.collect.trans.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.collect.PreviewForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.vo.collect.PreviewVo;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;
import com.pufferfishscheduler.master.collect.trans.engine.logchannel.LogChannel;

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
     * @param flow 转换流表单
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
     * 复制转换流
     *
     * @param id 转换流id
     * @return
     */
    void copyTrans(Integer id);

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
     * 获取转换流运行日志
     *
     * @param id 转换流id
     * @return 转换流运行日志
     */
    LogChannel getProcessLog(Integer id);

    /**
     * 展示转换流图片
     *
     * @param id 转换流id
     * @return 转换流图片base64编码
     */
    String showTransImg(Integer id);

    /**
     * 预览转换流数据
     *
     * @param config 转换流配置表单
     * @return 预览数据
     */
    PreviewVo preview(PreviewForm config);

    /**
     * 获取转换流字段流
     *
     * @param flowId 转换流id
     * @param config 转换流配置
     * @param stepName 转换流步骤名称
     * @param type 字段类型
     * @return 字段流
     */
    String[] getFieldStream(Integer flowId, String config, String stepName, Integer type);

    /**
     * 校验转换流程运行状态
     *
     * @param id 转换流id
     * @return 转换流程运行状态
     */
    Boolean checkTransStatus(Integer id);

    /**
     * 复制转换流
     *
     * @param id 转换流id
     * @return
     */
    void copy(Integer id);
}
