package com.pufferfishscheduler.master.collect.trans.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.domain.vo.collect.PreviewVo;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;

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
     * 添加转换流
     *
     * @param flow
     * @return
     */
    Integer addFlow(TransFlowForm flow);

    /**
     * 更新转换流
     *
     * @param flow
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
     * @param flow
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
     * 展示转换流图片
     *
     * @param id 转换流id
     * @return 转换流图片base64编码
     */
    String showImg(Integer id);

    /**
     * 预览转换流数据
     *
     * @param id 转换流id
     * @param stepName 步骤名称
     * @return 预览数据
     */
    PreviewVo preview(Integer id, String stepName);
}
