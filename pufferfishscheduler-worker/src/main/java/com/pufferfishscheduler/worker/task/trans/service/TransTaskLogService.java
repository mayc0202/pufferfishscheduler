package com.pufferfishscheduler.worker.task.trans.service;

import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingInfo;

/**
 * <p>
 * 任务执行日志，记录任务执行的详细信息，包括任务状态、任务参数、任务结果等。
 * <p>
 * 任务执行日志用于任务执行后的查询和分析，帮助用户了解任务执行情况和性能。
 *
 * @author Mayc
 * @since 2026-03-22  19:36
 */
public interface TransTaskLogService {

    /**
     * 同步任务执行日志
     *
     * @param jobExecutingInfo 任务执行信息
     */
    void syncTransTaskLog(JobExecutingInfo jobExecutingInfo);
}
