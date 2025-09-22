package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.Role;
import org.apache.ibatis.annotations.Mapper;

/**
 * (Role)表数据库访问层
 *
 * @author mayc
 * @since 2025-05-23 00:24:45
 */
@Mapper
public interface RoleMapper extends BaseMapper<Role> {

}
