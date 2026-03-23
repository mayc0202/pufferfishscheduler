package com.pufferfishscheduler.domain.form.collect;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 转换任务表单
 *
 * @author Mayc
 * @since 2026-03-20  16:57
 */
@Data
public class TransTaskForm {

    /**
     * 任务ID
     */
    private Integer id;

    /**
     * 任务名称
     */
    @NotBlank(message = "任务名称不能为空!")
    private String name;

    /**
     * 流程ID
     */
    @NotNull(message = "流程ID不能为空!")
    private Integer flowId;

    /**
     * 执行类型
     */
    @NotBlank(message = "调度方式不能为空!")
    private String executeType;

    /**
     * cron表达式
     */
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

    /**
     * 启用状态 0启用 1禁用（与元数据任务一致）
     */
    @NotNull(message = "启用状态不能为空!")
    private Boolean enable;

    /**
     * 备注
     */
    private String remark;

}
