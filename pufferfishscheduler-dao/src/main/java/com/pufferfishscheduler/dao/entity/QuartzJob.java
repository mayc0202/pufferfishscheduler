package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/**
 * 定时任务
 *
 * @author Mayc
 * @since 2025-09-11  01:30
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "quartz_job")
public class QuartzJob implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    // 任务id
    @TableId
    private Long jobId;

    @TableField(value = "job_name")
    private String jobName;

    // 任务分组
    @TableField(value = "job_group")
    private String jobGroup;

    // 调用目标
    @TableField(value = "invoke_target")
    private String invokeTarget;

    // cron表达式
    @TableField(value = "cron_expression")
    private String cronExpression;

    // 计划执行错误策略（1立即执行 2执行一次 3放弃执行）
    @TableField(value = "misfire_policy")
    private String misfirePolicy;

    // 是否并发执行（0允许 1禁止）
    @TableField(value = "concurrent")
    private String concurrent;

    // 状态（0正常 1暂停）
    @TableField(value = "status")
    private String status;

    // 备注信息
    @TableField(value = "remark")
    private String remark;
}
