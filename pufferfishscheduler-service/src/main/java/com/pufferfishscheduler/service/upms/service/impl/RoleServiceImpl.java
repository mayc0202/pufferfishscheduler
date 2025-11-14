package com.pufferfishscheduler.service.upms.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.dao.entity.Role;
import com.pufferfishscheduler.dao.mapper.RoleMapper;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import com.pufferfishscheduler.service.upms.service.RoleService;
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
}
