package com.pufferfishscheduler.api.dispatch;

import java.io.Serializable;

/**
 * Master -> Worker 任务调度消息（Kafka 方案B：无HTTP、单向投递）
 */
public class TaskDispatchMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String taskType; // metadata_task / realtime_task ...

    /**
     * 业务任务ID（例如 metadata_task.id）
     */
    private Integer taskId;

    /**
     * 数据源ID（例如 metadata_task.db_id）
     */
    private Integer dbId;

    /**
     * 计划触发时间（毫秒，要求秒级对齐：datetime(0)）
     */
    private Long scheduledTimeMillis;

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getDbId() {
        return dbId;
    }

    public void setDbId(Integer dbId) {
        this.dbId = dbId;
    }

    public Long getScheduledTimeMillis() {
        return scheduledTimeMillis;
    }

    public void setScheduledTimeMillis(Long scheduledTimeMillis) {
        this.scheduledTimeMillis = scheduledTimeMillis;
    }
}

