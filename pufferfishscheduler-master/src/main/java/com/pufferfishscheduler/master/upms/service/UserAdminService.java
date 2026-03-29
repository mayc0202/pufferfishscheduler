package com.pufferfishscheduler.master.upms.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.user.UserManageForm;
import com.pufferfishscheduler.domain.vo.user.RoleOptionVo;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.domain.vo.user.UserAdminVo;

import java.util.List;

/**
 * 用户管理（仅管理员）
 */
public interface UserAdminService {

    IPage<UserAdminVo> page(String account, String name, Integer pageNo, Integer pageSize);

    UserAdminVo detail(Integer id);

    List<RoleVo> listAssignableRoles();

    /**
     * 可分配角色下拉选项（含 disabled 字段给前端控制）
     */
    List<RoleOptionVo> listAssignableRoleOptions();

    void add(UserManageForm form);

    void update(UserManageForm form);

    /**
     * 注销用户（逻辑删除）并使其登录态失效
     */
    void deactivate(Integer id);

    /**
     * 验证当前用户是否为管理员
     */
    Boolean validateIsAdmin();
}
