package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.User;
import org.apache.ibatis.annotations.Mapper;

/**
 * (User)表数据库访问层
 *
 * @author mayc
 * @since 2025-05-23 00:24:45
 */
@Mapper
public interface UserMapper extends BaseMapper<User> {

}

