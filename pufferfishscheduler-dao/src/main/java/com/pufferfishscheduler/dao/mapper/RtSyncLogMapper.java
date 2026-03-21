package com.pufferfishscheduler.dao.mapper;

import java.util.List;
import java.util.Map;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.RtSyncLog;

/**
 * RTSyncLog的Mapper接口
 * 数据同步日志表（记录每个小时的数据同步情况）
 */
public interface RtSyncLogMapper extends BaseMapper<RtSyncLog> {

    /**
     * 根据任务ID、同步日期和同步小时查询日志
     * @param taskId 任务ID
     * @param syncDate 同步日期（yyyyMMdd）
     * @param syncHour 同步小时（0~23）
     * @return 同步日志
     */
    RtSyncLog selectByTaskAndTime(Integer taskId, Integer syncDate, Integer syncHour);

    /**
     * 根据任务ID和同步日期查询所有小时日志
     * @param taskId 任务ID
     * @param syncDate 同步日期（yyyyMMdd）
     * @return 同步日志列表
     */
    List<RtSyncLog> selectByTaskAndDate(Integer taskId, Integer syncDate);

    /**
     * 根据任务ID查询所有日志
     * @param taskId 任务ID
     * @return 同步日志列表
     */
    List<RtSyncLog> selectByTaskId(Integer taskId);

    /**
     * 根据表映射ID查询日志
     * @param tableMapperId 表映射ID
     * @return 同步日志列表
     */
    List<RtSyncLog> selectByTableMapperId(Integer tableMapperId);

    /**
     * 统计某任务某天的总数据量
     * @param taskId 任务ID
     * @param syncDate 同步日期（yyyyMMdd）
     * @return 统计数据
     */
    Map<String, Object> countDailyVolume(Integer taskId, Integer syncDate);

}
