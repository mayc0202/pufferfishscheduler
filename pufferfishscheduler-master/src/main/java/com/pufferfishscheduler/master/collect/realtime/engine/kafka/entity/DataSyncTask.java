package com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity;

import java.util.ArrayList;
import java.util.List;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.DataSourceAdapter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据同步任务
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataSyncTask {

    /**
     * 数据同步类型: 全量+增量
     */
    public static final String DATA_SYNC_TYPE_FULL = "1";

    /**
     * 数据同步类型: 增量同步
     */
    public static final String DATA_SYNC_TYPE_INCREMENT = "2";

    /**
     * 写入类型: 仅插入
     */
    public static final String WRITETYPE_ONLY_INSERT = "ONLY_INSERT";

    /**
     * 写入类型: 插入更新
     */
    public static final String WRITETYPE_INSERT_UPDATE = "INSERT_UPDATE";

    /**
     * 任务ID
     */
    private Integer taskId;

    /**
     * 运行时配置
     */
    private String runtimeConfig;

    /**
     * 数据源适配器-来源
     */
    private DataSourceAdapter sourceDatabase;

    /**
     * 数据源适配器-目标
     */
    private DataSourceAdapter targetDatabase;

    /**
     * 数据同步类型: 全量+增量 或 增量同步
     */
    private String dataSyncType;

    /**
     * Kafka Broker
     */
    private String brokers;

    /**
     * 表映射列表
     */
    private List<TableMapper> tableMappers;

    /**
     * 心跳机制是否启用
     */
    private Boolean heartbeatEnabled;

    /**
     * 心跳间隔（秒）
     */
    private Integer heartbeatInterval;

    public List<String> getTopics() {
        List<String> result = new ArrayList<String>();

        for (TableMapper tmapper : tableMappers) {
            result.add(sourceDatabase.buildTopicName(taskId, tmapper.getSourceTableName()));
        }

        return result;
    }

    /**
     * 获取选择了删除数据的目标表名列表
     *
     * @return
     */
    public List<String> getTargetTableNames() {

        List<String> result = new ArrayList<String>();

        for (TableMapper tmapper : tableMappers) {
            // 只处理选择了删除数据的表
            if (null != tmapper.getDeleteDataFlag() && tmapper.getDeleteDataFlag()) {
                result.add(tmapper.getTargetTableName());
            }
        }

        return result;
    }

    /**
     * 校验数据同步任务配置
     */
    public void valid() {
        if (taskId == null) {
            throw new BusinessException("任务ID不能为空！");
        }

        if (sourceDatabase == null) {
            throw new BusinessException("源库配置不能为空！");
        }
        sourceDatabase.valid();

        if (targetDatabase == null) {
            throw new BusinessException("目标库配置不能为空！");
        }
        targetDatabase.valid();

        if (tableMappers == null) {
            throw new BusinessException("源库表和目标库表映射关系不能为空！");
        }

        for (TableMapper mapper : tableMappers) {
            mapper.valid();
        }
    }
}
