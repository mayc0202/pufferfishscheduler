package com.pufferfishscheduler.master.upms.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.Role;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.domain.vo.user.UserRoleRowVo;

import java.util.List;

/**
 *
 * @author Mayc
 * @since 2025-10-08  07:17
 */
public interface RoleService extends IService<Role> {

    /**
     * 根据用户id获取角色集合
     *
     * @param userId
     * @return
     */
    List<RoleVo> getRoleListByUserId(Integer userId);

    /**
     * 可分配给用户的系统角色（管理员 / 操作员）
     */
    List<RoleVo> listAssignableRoles();

    /**
     * 批量查询用户角色
     */
    List<UserRoleRowVo> listRolesByUserIds(List<Integer> userIds);
}
