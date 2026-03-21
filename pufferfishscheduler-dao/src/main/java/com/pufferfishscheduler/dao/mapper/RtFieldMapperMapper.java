package com.pufferfishscheduler.dao.mapper;

import java.util.List;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.RtFieldMapper;
import org.apache.ibatis.annotations.Param;

/**
 * RTFieldMapper的Mapper接口
 * 同步字段映射关系
 */
public interface RtFieldMapperMapper extends BaseMapper<RtFieldMapper> {

    /**
     * 根据table_mapper_id查询字段映射列表
     * @param tableMapperId 表映射ID
     * @return 字段映射列表
     */
    List<RtFieldMapper> selectByTableMapperId(Integer tableMapperId);

    /**
     * 根据task_id查询字段映射列表
     * @param taskId 任务ID
     * @return 字段映射列表
     */
    List<RtFieldMapper> selectByTaskId(Integer taskId);

    /**
     * 根据table_mapper_id删除字段映射
     * @param tableMapperId 表映射ID
     * @return 删除结果
     */
    int deleteByTableMapperId(String tableMapperId);

    /**
     * 根据task_id删除字段映射
     * @param taskId 任务ID
     * @return 删除结果
     */
    int deleteByTaskId(Integer taskId);

    /**
     * 原生 SQL 批量插入
     */
    int insertBatch(@Param("list") List<RtFieldMapper> list);
}