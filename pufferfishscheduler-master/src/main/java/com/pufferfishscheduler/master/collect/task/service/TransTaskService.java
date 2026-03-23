package com.pufferfishscheduler.master.collect.task.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.collect.TransTaskForm;
import com.pufferfishscheduler.domain.vo.collect.TransTaskVo;

/**
 * 转换任务服务
 *
 * @author Mayc
 * @since 2026-03-20
 */
public interface TransTaskService {

    /**
     * 分页查询转换任务（含分组子树、名称模糊、状态、启用筛选）
     */
    IPage<TransTaskVo> list(Integer groupId, String name, String status, Boolean enable,
                            Integer pageNo, Integer pageSize);

    /**
     * 任务详情（含字典翻译）
     */
    TransTaskVo detail(Integer id);

    /**
     * 新增转换任务
     */
    void add(TransTaskForm taskForm);

    /**
     * 编辑转换任务
     */
    void update(TransTaskForm taskForm);

    /**
     * 删除转换任务（逻辑删除）
     */
    void delete(Integer id);

    /**
     * 启用任务
     */
    void enable(Integer id);

    /**
     * 停用任务
     */
    void disable(Integer id);

    /**
     * 立即执行：Master 抢占并投递 Kafka，由 Worker 执行转换
     */
    void immediatelyExecute(Integer id);

    /**
     * 立即停止：Master 停止并投递 Kafka，由 Worker 停止转换
     */
    void immediatelyStop(Integer id);

}
