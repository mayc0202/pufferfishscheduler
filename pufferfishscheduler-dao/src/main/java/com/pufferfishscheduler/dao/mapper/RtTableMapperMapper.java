package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.RtTableMapper;

import java.util.List;

/**
 * RTTableMapper的Mapper接口
 * 同步表映射关系
 */
public interface RtTableMapperMapper extends BaseMapper<RtTableMapper> {

    /**
     * 根据任务ID查询表映射列表
     * @param taskId 任务ID
     * @return 表映射列表
     */
    List<RtTableMapper> selectByTaskId(Integer taskId);

    /**
     * 根据源表ID查询表映射列表
     * @param sourceTableId 源表ID
     * @return 表映射列表
     */
    List<RtTableMapper> selectBySourceTableId(Integer sourceTableId);

    /**
     * 根据目标表ID查询表映射列表
     * @param targetTableId 目标表ID
     * @return 表映射列表
     */
    List<RtTableMapper> selectByTargetTableId(Integer targetTableId);

    /**
     * 根据任务ID删除表映射
     * @param taskId 任务ID
     * @return 删除结果
     */
    int deleteByTaskId(Integer taskId);

    /**
     * 根据源表ID和目标表ID查询表映射
     * @param sourceTableId 源表ID
     * @param targetTableId 目标表ID
     * @return 表映射
     */
    RtTableMapper selectBySourceAndTarget(Integer sourceTableId, Integer targetTableId);

}
