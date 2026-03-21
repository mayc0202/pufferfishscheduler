package com.pufferfishscheduler.domain.vo.user;

import lombok.Data;

/**
 * 用户与角色行（批量查询）
 */
@Data
public class UserRoleRowVo {
    /**
     * 用户 ID
     */
    private Integer userId;
    /**
     * 角色 ID
     */
    private Integer roleId;
    /**
     * 角色名称
     */
    private String roleName;
}
