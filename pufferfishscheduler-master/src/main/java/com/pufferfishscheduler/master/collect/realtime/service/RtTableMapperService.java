package com.pufferfishscheduler.master.collect.realtime.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.RtTableMapper;

import java.util.List;

/**
 * 实时数据同步任务-表映射服务接口
 *
 * @author Mayc
 * @since 2026-03-17  17:03:00
 */
public interface RtTableMapperService extends IService<RtTableMapper> {

    /**
     * 根据实时数据同步任务ID查询实时数据同步任务-表映射关系列表
     *
     * @param taskId 实时数据同步任务ID
     * @return 实时数据同步任务-表映射关系列表
     */
    List<RtTableMapper> selectByTaskId(Integer taskId);

    /**
     * 保存实时数据同步任务-表映射关系（不开启事务）
     *
     * @param rtTableMapper 实时数据同步任务-表映射关系
     */
    void saveWithoutTransaction(RtTableMapper rtTableMapper);

    /**
     * 根据实时数据同步任务ID删除实时数据同步任务-表映射关系
     *
     * @param taskId 实时数据同步任务ID
     */
    void deleteByTaskId(Integer taskId);
}
