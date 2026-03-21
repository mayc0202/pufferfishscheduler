package com.pufferfishscheduler.domain.vo.user;

import lombok.Data;

/**
 * 角色下拉选项（给前端使用：带 disabled）
 */
@Data
public class RoleOptionVo {

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
     * 角色描述
     * 下拉是否禁用（前端用于控制可选）
     */
    private Boolean disabled;
}
