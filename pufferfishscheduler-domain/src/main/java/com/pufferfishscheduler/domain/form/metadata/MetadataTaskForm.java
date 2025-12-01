package com.pufferfishscheduler.domain.form.metadata;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * 元数据同步任务表单
 *
 * @author Mayc
 * @since 2025-11-24  20:30
 */
@Data
public class MetadataTaskForm {

    @NotNull(message = "数据源id不能为空!")
    private Integer dbId;

    @NotBlank(message = "cron表达式不能为空!")
    private String cron;

    /**
     * 失败策略 0继续 1结束
     */
    @NotBlank(message = "失败策略不能为空!")
    private String failurePolicy = "1";

    /**
     * 通知策略 0都不发 1成功时通知 2失败时通知 3全部通知
     */
    @NotBlank(message = "通知策略不能为空!")
    private String notifyPolicy = "0";

    @NotNull(message = "工作组id不能为空!")
    private Integer workerId;

    /**
     * 启用状态 0启用 1禁用
     */
    @NotBlank(message = "启用状态不能为空!")
    private String enable;

    /**
     * 备注
     */
    private String remark;
}
