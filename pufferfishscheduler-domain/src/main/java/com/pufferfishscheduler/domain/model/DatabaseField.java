package com.pufferfishscheduler.domain.model;

import lombok.Data;
import java.util.Date;

/**
 *
 * @author Mayc
 * @since 2025-09-07  00:52
 */
@Data
public class DatabaseField {
    /**
     * 自增主键
     */
    private Integer id;

    /**
     * 表ID
     */
    private Integer tableId;

    private String name;

    /**
     * 业务名称
     */
    private String businessName;

    /**
     * 数据类型
     */
    private String dataType;

    /**
     * 数据长度
     */
    private Integer dataLength;

    /**
     * 字段描述
     */
    private String description;

    /**
     * 是否可以为空 0-否，1-是
     */
    private Boolean nullable;

    /**
     * 排序字段
     */
    private Integer orderBy;

    /**
     * 小数点精度
     */
    private Integer precision;

    /**
     * 是否删除，0-未删除，1-已删除，默认0
     */
    private Boolean deleted;

    /**
     * 创建人
     */
    private String createdBy;

    /**
     * 创建时间
     */
    private Date createdTime;

    /**
     * 更新人
     */
    private String updatedBy;

    /**
     * 更新时间
     */
    private Date updatedTime;
}
