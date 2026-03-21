package com.pufferfishscheduler.master.collect.realtime.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.dao.entity.RtTableMapper;
import com.pufferfishscheduler.dao.mapper.RtTableMapperMapper;
import com.pufferfishscheduler.master.collect.realtime.service.RtTableMapperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 实时数据同步任务-表映射服务实现类
 *
 * @author Mayc
 * @since 2026-03-17  17:05
 */
@Service
public class RtTableMapperServiceImpl extends ServiceImpl<RtTableMapperMapper, RtTableMapper> implements RtTableMapperService {

    @Autowired
    private RtTableMapperMapper rtTableMapperMapper;

    /**
     * 根据任务ID查询表映射列表
     *
     * @param taskId 任务ID
     * @return 表映射列表
     */
    @Override
    public List<RtTableMapper> selectByTaskId(Integer taskId) {
        if (taskId == null) {
            return List.of();
        }

        return rtTableMapperMapper.selectByTaskId(taskId);
    }

    /**
     * 保存实时数据同步任务-表映射关系（不开启事务）
     *
     * @param rtTableMapper 实时数据同步任务-表映射关系
     */
    @Override
    public void saveWithoutTransaction(RtTableMapper rtTableMapper) {
        rtTableMapperMapper.insert(rtTableMapper);
    }

    /**
     * 根据实时数据同步任务ID删除实时数据同步任务-表映射关系
     *
     * @param taskId 实时数据同步任务ID
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void deleteByTaskId(Integer taskId) {
        rtTableMapperMapper.deleteByTaskId(taskId);
    }
}
