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
 * 用户角色关联
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "user_role_rel")
public class UserRoleRel implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId
    private Integer id;

    @TableField("user_id")
    private Integer userId;

    @TableField("role_id")
    private Integer roleId;

    @TableField("deleted")
    private Boolean deleted;

    @TableField("created_by")
    private String createdBy;

    @TableField("created_time")
    private Date createdTime;

    @TableField("updated_by")
    private String updatedBy;

    @TableField("updated_time")
    private Date updatedTime;
}
