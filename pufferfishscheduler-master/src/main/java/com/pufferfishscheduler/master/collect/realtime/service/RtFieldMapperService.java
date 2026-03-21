package com.pufferfishscheduler.master.collect.realtime.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.RtFieldMapper;

import java.util.List;

/**
 * 实时数据同步任务-字段映射服务接口
 *
 * @author Mayc
 * @since 2026-03-17  17:04:00
 */
public interface RtFieldMapperService extends IService<RtFieldMapper> {

    /**
     * 根据实时数据同步任务-表映射ID查询实时数据同步任务-字段映射关系列表
     *
     * @param tableMapperId 实时数据同步任务-表映射ID
     * @return 实时数据同步任务-字段映射关系列表
     */
    List<RtFieldMapper> selectByTableMapperId(Integer tableMapperId);

    /**
     * 保存实时数据同步任务-字段映射关系（不开启事务）
     *
     * @param rtFieldMapper 实时数据同步任务-字段映射关系
     */
    void saveWithoutTransaction(RtFieldMapper rtFieldMapper);

    /**
     * 批量保存实时数据同步任务-字段映射关系（不开启事务）
     *
     * @param rtFieldMappers 实时数据同步任务-字段映射关系列表
     */
    void batchSaveWithoutTransaction(List<RtFieldMapper> rtFieldMappers);

    /**
     * 根据实时数据同步任务ID删除实时数据同步任务-字段映射关系
     *
     * @param taskId 实时数据同步任务ID
     */
    void deleteByTaskId(Integer taskId);
}
