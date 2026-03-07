package com.pufferfishscheduler.domain.model.quartz;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 定时任务详情
 *
 * @author Mayc
 * @since 2025-07-30  11:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QuartzJobDetail implements Serializable {
    private static final long serialVersionUID = 1L;

    // 任务id
    private Long jobId;

    // 任务名称
    private String jobName;

    // 任务分组
    private String jobGroup;

    // 调用目标
    private String invokeTarget;

    // cron表达式
    private String cronExpression;

    // 不匹配策略
    private String misfirePolicy;

    // 同时发生的
    private String concurrent;

    // 任务状态
    private String status;

}
