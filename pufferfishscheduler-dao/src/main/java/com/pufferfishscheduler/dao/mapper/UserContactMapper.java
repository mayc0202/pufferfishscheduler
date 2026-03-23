package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.UserContact;
import org.apache.ibatis.annotations.Mapper;

/**
 * 用户中心-联系人表数据库访问层
 */
@Mapper
public interface UserContactMapper extends BaseMapper<UserContact> {

}
