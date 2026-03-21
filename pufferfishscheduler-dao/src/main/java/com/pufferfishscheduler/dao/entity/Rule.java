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
 * rule
 * 规则表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rule")
public class Rule implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 规则ID(UUID)
     */
    @TableId
    private String id;

    /**
     * 规则分类ID
     */
    @TableField(value = "group_id")
    private Integer groupId;

    /**
     * 规则代码
     */
    @TableField(value = "rule_code")
    private String ruleCode;

    /**
     * 规则名称
     */
    @TableField(value = "rule_name")
    private String ruleName;

    /**
     * 规则描述
     */
    @TableField(value = "rule_description")
    private String ruleDescription;

    /**
     * 规则类型
     */
    @TableField(value = "rule_processor_id")
    private Integer ruleProcessorId;

    /**
     * 参数样式类型 1通用 2公共 3自定义
     */
    @TableField(value = "rule_type")
    private Integer ruleType;

    /**
     * 状态 0-草稿 1-发布
     */
    @TableField(value = "status")
    private Integer status;

    /**
     * 草稿配置
     */
    @TableField(value = "draft_config")
    private String draftConfig;

    /**
     * 发布后配置
     */
    @TableField(value = "released_config")
    private String releasedConfig;

    /**
     * 分类最高级的id
     */
    @TableField(value = "first_group_id")
    private Integer firstGroupId;

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
     * 更新人账号
     */
    @TableField(value = "updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;
}