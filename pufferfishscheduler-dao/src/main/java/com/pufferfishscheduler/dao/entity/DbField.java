package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * db_field
 */
@Data
@TableName(value = "db_field")
public class DbField implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 自增主键
     */
    @TableId
    private Integer id;

    /**
     * 表ID
     */
    @TableField(value = "table_id")
    private Integer tableId;

    @TableField(value = "`name`")
    private String name;

    /**
     * 业务名称
     */
    @TableField(value = "business_name")
    private String businessName;

    /**
     * 数据类型
     */
    @TableField(value = "data_type")
    private String dataType;

    /**
     * 数据长度
     */
    @TableField(value = "data_length")
    private Integer dataLength;

    /**
     * 字段描述
     */
    @TableField(value = "description")
    private String description;

    /**
     * 是否可以为空 0-否，1-是
     */
    @TableField(value = "nullable")
    private Boolean nullable;

    /**
     * 排序
     */
    @TableField(value = "order_by")
    private Integer orderBy;

    /**
     * 小数点精度
     */
    @TableField(value = "`precision`")
    private Integer precision;

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