package com.pufferfishscheduler.domain.vo.user;

import lombok.Data;

/**
 * @Author: yc
 * @CreateTime: 2025-05-27
 * @Description:
 * @Version: 1.0
 */
@Data
public class RoleVo {

    /**
     * 角色id
     */
    private Integer roleId;

    /**
     * 角色名称
     */
    private String roleName;

    /**
     * 角色描述
     */
    private String roleDesc;

    /**
     * 是否禁用
     */
    private Boolean disabled;
}