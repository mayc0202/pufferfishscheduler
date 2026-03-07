/*
*  Inc.
* Copyright (c) 2022 All Rights Reserved.
* create by 
* date:2022-08-30
*/
package com.pufferfishscheduler.dao.entity;


import com.baomidou.mybatisplus.annotation.IdType;
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
 * 转换配置表 Entity
 *
 * @author 
 * @date 2022-08-30 16:37:26
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "trans_flow")
public class TransFlow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.AUTO)
    private Integer id;

    /**
     * 流程名称
     */
	@TableField("name")
    private String name;

    /**
     * 描述
     */
	@TableField("description")
    private String description;

    /**
     * 参数配置
     */
    @TableField("param_config")
    private String paramConfig;

    /**
     * 配置
     */
    @TableField("config")
    private String config;

    /**
     * 路径
     */
	@TableField("path")
    private String path;

    /**
     * 分类id
     */
    @TableField("group_id")
    private Integer groupId;

    /**
     * 行大小
     */
    @TableField("row_size")
    private Integer rowSize;

    /**
     * 流程图
     */
    @TableField("image")
    private String image;

    /**
     * 步骤
     */
    @TableField("stage")
    private String stage;

    /**
     * 是否删除，0-否；1-是
     */
	@TableField("deleted")
    private Boolean deleted;

    /**
     * 创建人账号
     */
	@TableField("created_by")
    private String createdBy;

    /**
     * 创建时间
     */
	@TableField("created_time")
    private Date createdTime;

    /**
     * 更新人账号
     */
	@TableField("updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
	@TableField("updated_time")
    private Date updatedTime;

}
