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
import java.util.Date;

/**
 * rt_field_mapper
 * 同步字段映射关系
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rt_field_mapper")
public class RtFieldMapper implements Serializable {

    @Serial
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
     * rt_table_mapper表的id
     */
    @TableField(value = "table_mapper_id")
    private Integer tableMapperId;

    /**
     * 源表字段ID
     */
    @TableField(value = "source_field_id")
    private Integer sourceFieldId;

    /**
     * 源表字段名称
     */
    @TableField(value = "source_field_name")
    private String sourceFieldName;

    /**
     * 目标表字段ID
     */
    @TableField(value = "target_field_id")
    private Integer targetFieldId;

    /**
     * 目标表字段名称
     */
    @TableField(value = "target_filed_name")
    private String targetFiledName;

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