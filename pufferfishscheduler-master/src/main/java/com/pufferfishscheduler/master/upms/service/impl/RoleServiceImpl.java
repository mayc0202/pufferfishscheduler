package com.pufferfishscheduler.master.upms.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.Role;
import com.pufferfishscheduler.dao.mapper.RoleMapper;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.domain.vo.user.UserRoleRowVo;
import com.pufferfishscheduler.master.upms.service.RoleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Mayc
 * @since 2025-10-08  07:18
 */
@Service
public class RoleServiceImpl extends ServiceImpl<RoleMapper, Role> implements RoleService {

    @Autowired
    private RoleMapper roleMapper;

    /**
     * 根据用户id获取角色集合
     *
     * @param userId
     * @return
     */
    @Override
    public List<RoleVo> getRoleListByUserId(Integer userId) {
        if (Objects.isNull(userId)) {
            return Collections.emptyList();
        }
        // 查询用户角色
        return roleMapper.getRoleListByUserId(userId);
    }

    /**
     * 可分配给用户的系统角色（管理员 / 操作员）
     *
     * @return
     */
    @Override
    public List<RoleVo> listAssignableRoles() {
        LambdaQueryWrapper<Role> q = new LambdaQueryWrapper<>();
        q.eq(Role::getDeleted, Constants.DELETE_FLAG.FALSE)
                .in(Role::getName, Constants.ROLE_NAME.ADMIN, Constants.ROLE_NAME.EDITOR)
                .orderByAsc(Role::getId);
        List<Role> roles = list(q);
        return roles.stream().map(r -> {
            RoleVo vo = new RoleVo();
            vo.setRoleId(r.getId());
            vo.setRoleName(r.getName());
            vo.setDisabled(r.getDisabled());
            return vo;
        }).toList();
    }

    @Override
    public List<UserRoleRowVo> listRolesByUserIds(List<Integer> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Collections.emptyList();
        }
        return roleMapper.listRolesByUserIds(userIds);
    }
}
