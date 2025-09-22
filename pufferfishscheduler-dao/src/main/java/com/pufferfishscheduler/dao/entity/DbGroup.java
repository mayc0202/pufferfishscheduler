package com.pufferfishscheduler.dao.entity;


import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * (DbGroup)表实体类
 *
 * @author mayc
 * @since 2025-05-22 00:00:40
 */
@Data
@TableName(value = "db_group")
public class DbGroup implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId
    private Integer id;

    /**
     * 数据库名称
     */
    @TableField(value = "`name`")
    private String name;

    /**
     * 排序
     */
    @TableField(value = "order_by")
    private Integer orderBy;

    /**
     * 是否解除：0-未解除；1-已解除，默认未删除
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

