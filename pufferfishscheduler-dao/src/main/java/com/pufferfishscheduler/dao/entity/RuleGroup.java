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
 * rule_group
 * 规则分类表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rule_group")
public class RuleGroup implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 规则分类主键
     */
    @TableId
    private Integer id;

    /**
     * 分组名称
     */
    @TableField(value = "group_name")
    private String groupName;

    /**
     * 上级分组ID
     */
    @TableField(value = "parent_id")
    private Integer parentId;

    /**
     * 排序字段
     */
    @TableField(value = "order_by")
    private Integer orderBy;

    /**
     * 规则类型(0-公共 1-自定义)
     */
    @TableField(value = "group_type")
    private Integer groupType;

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
     * 修改人账号
     */
    @TableField(value = "updated_by")
    private String updatedBy;

    /**
     * 修改时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;
}