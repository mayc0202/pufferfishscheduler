package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.Role;
import com.pufferfishscheduler.domain.vo.user.RoleVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * (Role)表数据库访问层
 *
 * @author mayc
 * @since 2025-05-23 00:24:45
 */
@Mapper
public interface RoleMapper extends BaseMapper<Role> {

    /**
     * 根据用户id获取角色列表
     * @param userId
     * @return
     */
    List<RoleVo> getRoleListByUserId(@Param("userId")Integer userId);
}
