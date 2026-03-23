package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.dao.entity.TransTaskLog;
import com.pufferfishscheduler.domain.vo.collect.TransTaskLogVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Map;

@Mapper
public interface TransTaskLogMapper extends BaseMapper<TransTaskLog> {
    /**
     * 分页查询转换任务日志
     *
     * @param page 分页参数
     * @param params 查询参数
     * @return 分页结果
     */
    IPage<TransTaskLogVo> selectTaskLogList(Page<TransTaskLogVo> page,
                                            @Param("params") Map<String, Object> params);

    /**
     * 根据日志ID查询转换任务日志详情
     *
     * @param id 日志ID
     * @return 转换任务日志VO
     */
    TransTaskLogVo detail(@Param("id") String id);
}

