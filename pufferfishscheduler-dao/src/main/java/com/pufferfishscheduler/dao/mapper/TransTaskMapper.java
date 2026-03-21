package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.domain.vo.collect.TransTaskVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Map;

@Mapper
public interface TransTaskMapper extends BaseMapper<TransTask> {

    /**
     * 分页查询转换任务
     */
    IPage<TransTaskVo> selectTaskList(IPage<TransTaskVo> page,
                                      @Param("params") Map<String, Object> params);

    /**
     * 查询转换任务详情
     */
    TransTaskVo selectTaskById(@Param("id") Integer id);
}

