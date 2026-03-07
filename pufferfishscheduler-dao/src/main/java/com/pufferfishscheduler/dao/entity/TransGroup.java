package com.pufferfishscheduler.dao.entity;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 转换分组
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "trans_group")
public class TransGroup implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 分类主键
     */
    @TableId(type = IdType.AUTO)
    private Integer id;

    /**
     * 分组名称
     */
    @TableField("name")
    private String name;

    /**
     * 上级分组ID
     */
    @TableField("parent_id")
    private Integer parentId;

    /**
     * 排序字段
     */
    @TableField("order_by")
    private Integer orderBy;

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