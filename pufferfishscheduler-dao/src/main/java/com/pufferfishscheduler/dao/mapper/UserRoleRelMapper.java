package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.UserRoleRel;
import org.apache.ibatis.annotations.Mapper;

/**
 * 用户角色关联表
 */
@Mapper
public interface UserRoleRelMapper extends BaseMapper<UserRoleRel> {
}
