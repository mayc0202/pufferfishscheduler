package com.pufferfishscheduler.dao.entity;


import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * (DbBasic)表实体类
 *
 * @author mayc
 * @since 2025-05-20 23:34:42
 */
@Data
@TableName(value = "db_basic")
public class DbBasic implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId
    private Integer id;

    /**
     * 数据库名称
     */
    @TableField(value = "`name`")
    private String name;

    /**
     * logo图片
     */
    @TableField(value = "img")
    private String img;

    /**
     * 图片配置
     *
     */
    @TableField(value = "img_config")
    private String imgConfig;

    /**
     * 数据库类型id
     */
    @TableField(value = "category_id")
    private Integer categoryId;

    /**
     * 排序
     */
    @TableField(value = "order_by")
    private Integer orderBy;

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

