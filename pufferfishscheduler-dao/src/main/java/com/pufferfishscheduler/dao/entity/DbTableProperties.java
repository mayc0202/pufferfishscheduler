package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 数据库表属性实体类
 *
 * @author Mayc
 * @since 2026-03-17  18:20
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DbTableProperties {

    @TableField("id")
    private String id;

    @TableField("table_id")
    private Integer tableId;

    @TableField("prop_type")
    private String propType;      // 属性类型：1-物理主键，2-逻辑主键等

    @TableField("`prop_values`")
    private String propValues;     // 属性值

    @TableField("created_by")
    private String createdBy;

    @TableField("created_time")
    private Date createdTime;
}
