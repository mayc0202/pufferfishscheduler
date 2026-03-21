package com.pufferfishscheduler.master.collect.realtime.engine.kafka.data;

import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.dao.mapper.RtTableStatsMapper;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.RealTimeDataSyncStatsTask;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实时同步任务数据
 *
 * @author Mayc
 * @since 2026-03-15  23:05
 */
@Data
@Slf4j
public class RealTimeSyncTaskData {

    /**
     * 任务ID
     */
    private Integer taskId;

    /**
     * 实时同步任务-统计任务运行结果
     */
    private RealTimeDataSyncStatsTask realTimeDataSyncStatsTask;


    public RealTimeSyncTaskData(Integer taskId) {
        this.taskId = taskId;
        log.info(String.format("开始重构任务【任务ID：%s】的内存统计信息...", taskId));
    }

    /**
     * 停止并释放实时统计任务，供 RealTimeSyncTaskDatas 在注销任务时调用。
     */
    public void stopRealTimeDataSyncStatsTaskQuietly() {
        if (realTimeDataSyncStatsTask == null) {
            return;
        }
        try {
            realTimeDataSyncStatsTask.shutdown();
        } catch (Exception e) {
            log.warn("停止实时统计任务失败, taskId={}", taskId, e);
        }
    }
}
