package com.pufferfishscheduler.domain.vo.dashboard;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * 首页仪表盘汇总数据
 *
 * 面向前端首页一次性聚合统计，避免多接口拼装带来的延迟与口径不一致。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DashboardSummaryVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 顶部卡片：任务总量/分类任务数
     */
    private TaskCards taskCards;

    /**
     * 任务状态分布（按模块）
     * key 为模块标识：metadata/trans/clean/realtime
     */
    private Map<String, StatusDistribution> statusDistributions;

    /**
     * 模块总进度/完成率（按模块）
     * key 为模块标识：metadata/trans/clean/realtime
     */
    private Map<String, Progress> progress;

    /**
     * 知识库概览
     */
    private KnowledgeOverview knowledgeOverview;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskCards implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private long totalTasks;
        private long metadataTasks;
        private long cleanTasks;
        private long realtimeTasks;
        private long transTasks;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatusDistribution implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * key 为状态码（如 INIT/RUNNING/STARTING/STOPPING/STOP/FAILURE），value 为数量
         */
        private Map<String, Long> counts;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Progress implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * 进度百分比：0-100
         */
        private int percent;

        /**
         * 失败数（用于前端显示/诊断）
         */
        private long failureCount;

        /**
         * 总数
         */
        private long totalCount;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KnowledgeOverview implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private long knowledgeCount;
        private long categoryCount;
        private long relationCount;
        private long tagCount;
    }
}

