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

/**
 * rule_processor
 * 规则处理器表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rule_processor")
public class RuleProcessor implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 处理器ID
     */
    @TableId
    private Integer id;

    /**
     * 规则类型
     */
    @TableField(value = "processor_name")
    private String processorName;

    /**
     * 规则处理器Java类的全路径
     */
    @TableField(value = "processor_class")
    private String processorClass;

    /**
     * 规则的参数定义
     */
    @TableField(value = "params_config")
    private String paramsConfig;

    /**
     * 样式类型。1-通用；2-公共；3-自定义
     */
    @TableField(value = "style_type")
    private Integer styleType;
}