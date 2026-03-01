package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.QuartzJob;
import org.apache.ibatis.annotations.Mapper;

/**
 *
 * @author Mayc
 * @since 2025-09-11  01:35
 */
@Mapper
public interface QuartzJobMapper extends BaseMapper<QuartzJob> {
}
