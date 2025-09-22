package com.pufferfishscheduler.domain.model;

import lombok.Data;
import java.util.Date;

/**
 *
 * @author Mayc
 * @since 2025-09-07  00:52
 */
@Data
public class DatabaseTable {
    /**
     * 自增主键
     */
    private Integer id;

    /**
     * 数据库ID
     */
    private Integer dbId;

    private String name;

    /**
     * 业务名称
     */
    private String businessName;

    /**
     * 描述
     */
    private String description;

    /**
     * 是否删除，0未删除，1已删除，默认0
     */
    private Boolean deleted;

    /**
     * 创建人账号
     */
    private String createdBy;

    /**
     * 创建时间
     */
    private Date createdTime;

    /**
     * 更新人账号
     */
    private String updatedBy;

    /**
     * 更新时间
     */
    private Date updatedTime;

}
