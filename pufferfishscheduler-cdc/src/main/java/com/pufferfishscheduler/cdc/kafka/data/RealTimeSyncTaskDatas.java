package com.pufferfishscheduler.cdc.kafka.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于存放实时同步任务信息（只负责任务注册/反注册和统计线程的生命周期管理）。
 */
public class RealTimeSyncTaskDatas {

    private static final Logger logger = LoggerFactory.getLogger(RealTimeSyncTaskDatas.class);
    private static final Map<Integer, RealTimeSyncTaskData> taskDataMap = new ConcurrentHashMap<>();

    private RealTimeSyncTaskDatas() {
        // 私有构造函数，防止实例化
    }

    /**
     * 任务运行成功后注册任务，用于后续资源管理
     */
    public static void register(RealTimeSyncTaskData taskData) {
        taskDataMap.put(taskData.getTaskId(), taskData);
    }

    /**
     * 任务停止、删除时调用，解除注册并释放资源
     */
    public static void unRegister(Integer taskId) {
        RealTimeSyncTaskData taskData = taskDataMap.get(taskId);
        if (taskData == null) {
            return;
        }

        logger.info("停止实时统计任务，任务号：{}", taskId);
        taskData.stopRealTimeDataSyncStatsTaskQuietly();
        taskDataMap.remove(taskId);
    }

    public static RealTimeSyncTaskData getTask(Integer taskId) {
        return taskDataMap.get(taskId);
    }
}