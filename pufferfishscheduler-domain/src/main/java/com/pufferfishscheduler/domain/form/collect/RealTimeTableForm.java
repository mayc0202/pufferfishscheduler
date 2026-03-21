package com.pufferfishscheduler.domain.form.collect;

import java.util.List;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

/**
 * 实时数据同步表表单
 */
@Data
public class RealTimeTableForm {

    private Integer id;

    /**
     * 源表ID
     */
    @NotNull(message = "源表ID不能为空")
    private Integer sourceTableId;

    /**
     * 源表名称
     */
    @NotBlank(message = "源表名称不能为空")
    private String sourceTableName;

    /**
     * 目标表ID
     */
    @NotNull(message = "目标表ID不能为空")
    private Integer targetTableId;

    /**
     * 目标表名称
     */
    @NotBlank(message = "目标表名称不能为空")
    private String targetTableName;

    /**
     * 任务首次启动后是否删除数据。true - 任务首次启动删除目标库中目标表的数据； false - 不删除目标库目标表数据。默认：false
     */
    @NotNull(message = "任务首次启动后是否删除数据不能为空!")
    private Boolean deleteFlag;

    /**
     * 是否并行写入。true - 并行写入； false - 顺序写入。默认：false
     */
    @NotNull(message = "是否并行写入不能为空!")
    private Boolean parallelWriteFlag;

    /**
     * 并行写入线程数。只有当parallel_write_flag 为true的时候，此值才会必填。默认值是5，最大值10
     */
    @NotNull(message = "并行写入线程数不能为空!")
    private Integer parallelThreadNum;

    /**
     * 写入类型。INSERT - 插入； UPSERT - 插入或更新； DELETE - 删除。默认：INSERT
     */
    @NotBlank(message = "写入类型不能为空!")
    private String writeType;

    /**
     * 写入批次大小。默认：1000
     */
    private Integer batchSize = 1000;

    /**
     * 字段映射列表
     */
    private List<RealTimeFieldMapperForm> fieldMappers;
}
