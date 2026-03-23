package com.pufferfishscheduler.master.collect.task.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.vo.collect.TransTaskLogVo;

/**
 * 转换任务日志服务层
 *
 * @author Mayc
 * @since 2026-03-23  16:04
 */
public interface TransTaskLogService {

    /**
     * 分页查询转换任务日志
     *
     * @param taskName  任务名称
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param status    状态
     * @param pageNo    页码
     * @param pageSize  每页数量
     * @return 分页结果
     */
    IPage<TransTaskLogVo> list(String taskName, String startTime, String endTime, String status, Integer pageNo, Integer pageSize);

    /**
     * 查询转换任务日志详情
     *
     * @param id 日志ID
     * @return 日志详情
     */
    TransTaskLogVo detail(String id);
}
