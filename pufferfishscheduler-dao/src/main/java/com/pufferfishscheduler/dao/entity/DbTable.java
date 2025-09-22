package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * db_table
 */
@Data
@TableName(value = "db_table")
public class DbTable implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 自增主键
     */
    @TableId
    private Integer id;

    /**
     * 数据库ID
     */
    @TableField(value = "db_id")
    private Integer dbId;

    @TableField(value = "`name`")
    private String name;

    /**
     * 业务名称
     */
    @TableField(value = "business_name")
    private String businessName;

    /**
     * 描述
     */
    @TableField(value = "description")
    private String description;

    /**
     * 是否删除，0未删除，1已删除，默认0
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人
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