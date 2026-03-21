package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * rt_table_mapper
 * 同步表映射关系
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rt_table_mapper")
public class RtTableMapper implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键
     */
    @TableId
    private Integer id;

    /**
     * 任务ID。rt_task表id
     */
    @TableField(value = "task_id")
    private Integer taskId;

    /**
     * 源表ID
     */
    @TableField(value = "source_table_id")
    private Integer sourceTableId;

    /**
     * 源表名称
     */
    @TableField(value = "source_table_name")
    private String sourceTableName;

    /**
     * 目标表ID
     */
    @TableField(value = "target_table_id")
    private Integer targetTableId;

    /**
     * 目标表名称
     */
    @TableField(value = "target_table_name")
    private String targetTableName;

    /**
     * 任务首次启动后是否删除数据。1 - 任务首次启动删除目标库中目标表的数据； 0 - 不删除目标库目标表数据。默认：否
     */
    @TableField(value = "delete_data_flag")
    private Boolean deleteDataFlag;

    /**
     * 是否并发写入。1 - 是；0 - 否。 默认：否
     */
    @TableField(value = "parallel_write_flag")
    private Boolean parallelWriteFlag;

    /**
     * 并发写入线程数量。只有当parallel_write_flag = 1的时候，此值才会必填。默认值是5，最大值10
     */
    @TableField(value = "parallel_thread_num")
    private Integer parallelThreadNum;

    /**
     * 数据写入方式。可以选择 ：ONLY_INSERT - 仅插入、INSERT_UPDATE - 插入/更新
     */
    @TableField(value = "write_type")
    private String writeType;

    /**
     * 批量提交批次大小。默认：1000
     */
    @TableField(value = "batch_size")
    private Integer batchSize;

    /**
     * 是否删除，0-否；1-是
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人账号
     */
    @TableField(value = "created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time")
    private Date createdTime;

    /**
     * 更新人
     */
    @TableField(value = "updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;

}
