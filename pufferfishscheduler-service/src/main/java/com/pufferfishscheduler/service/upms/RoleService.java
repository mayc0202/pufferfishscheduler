package com.pufferfishscheduler.service.upms;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.Role;
import com.pufferfishscheduler.domain.vo.user.RoleVo;

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
}
