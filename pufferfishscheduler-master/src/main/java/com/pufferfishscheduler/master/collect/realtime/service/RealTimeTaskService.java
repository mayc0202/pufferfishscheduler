package com.pufferfishscheduler.master.collect.realtime.service;

import java.util.List;
import java.util.Map;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.dao.entity.RtTask;
import com.pufferfishscheduler.domain.form.collect.RealTimeTaskForm;
import com.pufferfishscheduler.domain.vo.collect.RealTimeTaskVo;
import com.pufferfishscheduler.cdc.kafka.entity.RealTimeSyncTaskStatus;

/**
 * 实时数据同步服务
 *
 * @author Mayc
 * @since 2026-03-13 17:44
 */
public interface RealTimeTaskService {

    /**
     * 查询数据同步任务列表
     *
     * @param taskName   任务名称
     * @param sourceDbId 源数据库ID
     * @param targetDbId 目标数据库ID
     * @param taskStatus 任务状态
     * @param pageNo     页码
     * @param pageSize   每页数量
     * @return 数据同步任务VO列表
     */
    IPage<RealTimeTaskVo> list(String taskName, Integer sourceDbId, Integer targetDbId, String taskStatus,
                               Integer pageNo, Integer pageSize);

    /**
     * 根据任务状态查询任务列表
     *
     * @param taskStatus 任务状态列表
     * @return 数据同步任务实体列表
     */
    List<RtTask> getTaskListByStatus(List<String> taskStatus);

    /**
     * 新增数据同步任务
     *
     * @param taskForm 任务表单
     */
    void add(RealTimeTaskForm taskForm);

    /**
     * 更新数据同步任务
     *
     * @param taskForm 任务表单
     */
    void update(RealTimeTaskForm taskForm);

    /**
     * 删除数据同步任务
     *
     * @param taskId 任务ID
     */
    void delete(Integer taskId);

    /**
     * 查询数据同步任务详情
     *
     * @param taskId 任务ID
     * @return 数据同步任务VO
     */
    RealTimeTaskVo detail(Integer taskId);

    /**
     * 立即启动数据同步任务（仅未启动/已停止/失败/异常状态可启动）
     *
     * @param taskId 任务ID
     */
    void immediatelyStart(Integer taskId);

    /**
     * 立即停止数据同步任务（仅运行中状态可停止）
     *
     * @param taskId 任务ID
     */
    void immediatelyStop(Integer taskId);

    /**
     * 启动数据同步任务
     *
     * @param taskId 任务ID
     */
    void start(Integer taskId);

    /**
     * 停止数据同步任务
     *
     * @param taskId 任务ID
     */
    void stop(Integer taskId);

    /**
     * 定时任务调度失败时，标记任务失败并记录原因
     *
     * @param taskId 任务ID
     * @param reason 失败原因
     */
    void markFailedFromScheduler(Integer taskId, String reason);

    /**
     * 更新任务状态（独立事务 REQUIRES_NEW）
     *
     * @param taskId        任务ID
     * @param newStatus     新状态
     * @param reason        失败原因
     * @param operator      操作人
     * @param runtimeConfig 运行时配置
     * @return 受影响行数（0 表示未更新，例如记录不存在或条件不匹配）
     */
    int updateTaskStatus(Integer taskId, String newStatus, String reason,
                         String operator, String runtimeConfig);

    /**
     * 查询实时累计统计：优先 Redis，无数据时读表 {@code rt_table_stats}（与 {@link com.pufferfishscheduler.cdc.kafka.RealtimeStatsRedisSyncJob} 同步目标一致）。
     *
     * @param taskId        任务ID
     * @param tableMapperId 表映射ID（rt_table_mapper.id）
     * @return last_idv/last_udv/last_ddv、today_*、updated_at 等键，与 Redis 结构一致
     */
    Map<String, Object> getCumulativeStats(Integer taskId, Integer tableMapperId);

    /**
     * 查询某小时实时统计：优先 Redis（key 存在时），否则读表 {@code rt_sync_log}。
     *
     * @param taskId        任务ID
     * @param tableMapperId 表映射ID
     * @param syncDate      日期 yyyyMMdd
     * @param syncHour      小时 0~23
     * @return insert_data_volume / update_data_volume / delete_data_volume
     */
    Map<String, Long> getHourlyStats(Integer taskId, Integer tableMapperId, int syncDate, int syncHour);

    /**
     * 获取数据同步任务真实状态
     *
     * @param id 任务ID
     * @return 数据同步任务真实状态
     */
    RealTimeSyncTaskStatus getTaskRealStatus(Integer id);
}
