package com.pufferfishscheduler.dao.mapper;

import java.util.List;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.RtTableStats;

/**
 * RtTableStats的Mapper接口
 * 表数据量统计
 * 每天结转一下这张表。last表示前一天的数据总量；today表示当天产生的数据总量。进入下一天时，上一天的总量=last_idv + today_idv
 */
public interface RtTableStatsMapper extends BaseMapper<RtTableStats> {

    /**
     * 根据任务ID查询表统计列表
     * @param taskId 任务ID
     * @return 表统计列表
     */
    List<RtTableStats> selectByTaskId(Integer taskId);

    /**
     * 根据表映射ID查询表统计
     * @param id 表映射ID
     * @return 表统计
     */
    RtTableStats selectByTableMapperId(Integer id);

    /**
     * 更新当天的统计数据
     * @param id 表映射ID
     * @param insertVolume 插入数据量
     * @param updateVolume 更新数据量
     * @param deleteVolume 删除数据量
     * @return 更新结果
     */
    int updateTodayStats(Integer id, Long insertVolume, Long updateVolume, Long deleteVolume);

    /**
     * 结转数据（将today数据移到last，today清零）
     * @param id 表映射ID
     * @return 结转结果
     */
    int carryOverStats(Integer id);

    /**
     * 批量结转数据（进入下一天时执行）
     * @return 结转结果
     */
    int batchCarryOverStats();

    /**
     * 获取总数据量（last + today）
     * @param id 表映射ID
     * @return 总数据量统计
     */
    java.util.Map<String, Object> getTotalVolume(Integer id);

}
