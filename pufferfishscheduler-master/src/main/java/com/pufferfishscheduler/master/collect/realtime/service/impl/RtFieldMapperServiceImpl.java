package com.pufferfishscheduler.master.collect.realtime.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.dao.entity.RtFieldMapper;
import com.pufferfishscheduler.dao.mapper.RtFieldMapperMapper;
import com.pufferfishscheduler.master.collect.realtime.service.RtFieldMapperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 实时数据同步任务-字段映射服务实现类
 *
 * @author Mayc
 * @since 2026-03-17  17:06
 */
@Service
public class RtFieldMapperServiceImpl extends ServiceImpl<RtFieldMapperMapper, RtFieldMapper> implements RtFieldMapperService {

    @Autowired
    private RtFieldMapperMapper rtFieldMapperMapper;

    /**
     * 根据实时数据同步任务-表映射ID查询实时数据同步任务-字段映射关系列表
     *
     * @param tableMapperId 实时数据同步任务-表映射ID
     * @return 实时数据同步任务-字段映射关系列表
     */
    @Override
    public List<RtFieldMapper> selectByTableMapperId(Integer tableMapperId) {
        if (tableMapperId == null) {
            return List.of();
        }

        return rtFieldMapperMapper.selectByTableMapperId(tableMapperId);
    }

    /**
     * 保存实时数据同步任务-字段映射关系（不开启事务）
     *
     * @param rtFieldMapper 实时数据同步任务-字段映射关系
     */
    @Override
    public void saveWithoutTransaction(RtFieldMapper rtFieldMapper) {
        rtFieldMapperMapper.insert(rtFieldMapper);
    }

    /**
     * 批量保存实时数据同步任务-字段映射关系（不开启事务）
     *
     * @param rtFieldMappers 实时数据同步任务-字段映射关系列表
     */
    @Override
    public void batchSaveWithoutTransaction(List<RtFieldMapper> rtFieldMappers) {
        rtFieldMapperMapper.insertBatch(rtFieldMappers);
    }

    /**
     * 根据实时数据同步任务ID删除实时数据同步任务-字段映射关系
     *
     * @param taskId 实时数据同步任务ID
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void deleteByTaskId(Integer taskId) {
        rtFieldMapperMapper.deleteByTaskId(taskId);
    }
}
