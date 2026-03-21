package com.pufferfishscheduler.domain.form.collect;

import java.util.List;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 实时任务表单
 */
@Data
public class RealTimeTaskForm {

    /**
     * 实时数据同步任务ID
     */
    private Integer taskId;

    /**
     * 任务名称
     */
    @NotBlank(message = "任务名称不能为空!")
    private String taskName;

    /**
     * 源数据库ID
     */
    @NotBlank(message = "来源数据库ID不能为空!")
    private Integer sourceDbId;

    /**
     * 目标数据库ID
     */
    @NotBlank(message = "目标数据库ID不能为空!")
    private Integer targetDbId;

    /**
     * 同步引擎
     */
    @NotBlank(message = "同步引擎不能为空!")
    private String engineType;

    /**
     * 同步类型
     */
    @NotBlank(message = "同步类型不能为空!")
    private String syncType;

    /**
     * 目标表名
     */
    @NotBlank(message = "来源表ID不能为空!")
    private Integer sourceTableId;

    /**
     * 目标表ID
     */
    @NotBlank(message = "目标表ID不能为空!")
    private Integer targetTableId;

    /**
     * 实时数据同步表映射列表
     */
    private List<RealTimeTableForm> tableMappers;

}
