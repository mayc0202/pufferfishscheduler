package com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.adapter;

import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.config.KafkaDataProperties;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.DataSourceAdapter;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.DataSyncTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Oracle 数据库适配器
 *
 * @author Mayc
 * @since 2026-03-16  12:01
 */
@Slf4j
public class OracleAdapter extends DataSourceAdapter {

    /**
     * 清空指定表数据
     */
    @Override
    public void truncateTables(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }

        try (Connection conn = JdbcUtil.getConnection(JdbcUtil.getDriver(connectionInfo.getType()), getJdbcUrlString(false), connectionInfo)) {
            for (String table : tables) {
                String sql = String.format("truncate table \"%s\"", table);
                log.info("清理目标【{}】表数据！SQL: {}", table, sql);
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.executeUpdate();
                } catch (Exception e) {
                    log.error("清理表数据失败, table={}", table, e);
                    throw new BusinessException(e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("清理 Oracle 目标表数据失败", e);
            throw new BusinessException(e.getMessage());
        }
    }

    /**
     * 构建 Oracle 源连接器配置
     *
     * @param task 数据同步任务
     * @return
     */
    @Override
    public NewConnectorDefinition buildSourceConnector(DataSyncTask task) {
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);
        KafkaDataProperties kafkaDataProperties = PufferfishSchedulerApplicationContext.getBean(KafkaDataProperties.class);
        NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()
                // 使用首字母 S + 任务ID 作为连接器名称
                .withName(getSourceConnectorName(task.getTaskId()))
                .withConfig("connector.class", "io.debezium.connector.oracle.OracleConnector")
                .withConfig("tasks.max", "1");

        // 源库配置
        builder.withConfig("database.hostname", task.getSourceDatabase().getConnectionInfo().getDbHost())
                .withConfig("database.port", task.getSourceDatabase().getConnectionInfo().getDbPort())
                .withConfig("database.user", task.getSourceDatabase().getConnectionInfo().getUsername())
                .withConfig("database.password", aesUtil.decrypt(connectionInfo.getPassword()))
                // 实时任务的主键（数字类型），使用任务ID作为服务ID
                .withConfig("database.server.id", task.getTaskId())
                // 实时任务的 UUID 值：SERVER_ + 任务ID
                .withConfig("database.server.name", getServerName(task.getTaskId()))
                .withConfig("database.dbname", task.getSourceDatabase().getConnectionInfo().getDbName())
                .withConfig("schema.include.list", task.getSourceDatabase().getConnectionInfo().getDbSchema())
                // 默认全量 + 增量
                .withConfig("snapshot.mode", "initial");

        if (DataSyncTask.DATA_SYNC_TYPE_INCREMENT.equals(task.getDataSyncType())) {
            // 增量：只做 schema 快照
            builder.withConfig("snapshot.mode", "schema_only");
        }

        String schema = task.getSourceDatabase().getConnectionInfo().getDbSchema();
        String keys = getSourceKeyFullPathsString(schema, task.getTableMappers());
        if (StringUtils.isNotEmpty(keys)) {
            builder.withConfig("message.key.columns", keys);
        }

        builder.withConfig("tombstones.on.delete", "true")
                .withConfig("topic.prefix", getServerName(task.getTaskId()));

        if (Boolean.TRUE.equals(task.getHeartbeatEnabled())) {
            // 启用心跳机制，没有填写心跳间隔，默认一小时
            String interval = task.getHeartbeatInterval() != null
                    ? task.getHeartbeatInterval().toString()
                    : "3600000";
            builder.withConfig("heartbeat.interval.ms", interval);
        }

        // 需要同步的表清单。schema.tableName 形式
        builder.withConfig("table.include.list",
                        getSourceTableFullPathsString(schema, task.getTableMappers()))
                // 需要同步的字段列表，多个字段之间使用逗号隔开。schema.tableName.columnName
                .withConfig("column.include.list",
                        getSourceColumnFullPathsString(schema, task.getTableMappers()))
                // Kafka 历史表配置
                .withConfig("database.history.kafka.bootstrap.servers", task.getBrokers())
                .withConfig("database.history.kafka.topic",
                        getHistoryTopicName(task.getTaskId(),
                                task.getSourceDatabase().getConnectionInfo().getDbName()))
                .withConfig("log.mining.strategy", "online_catalog")
                // 性能优化参数
                .withConfig("connector.client.config.override.policy", "all")
                .withConfig("topic.creation.default.replication.factor", config.topicReplicationFactor)
                .withConfig("topic.creation.default.partitions", config.topicPartitions)
                .withConfig("topic.creation.default.compression.type", config.topicCompressionType)
                .withConfig("producer.override.compression.acks", config.topicAcks)
                .withConfig("database.history.skip.unparseable.ddl", "true")
                .withConfig("database.history.store.only.monitored.tables.ddl", "true");
        if (kafkaDataProperties.getTimePrecisionMode() != null) {
            builder.withConfig("time.precision.mode", kafkaDataProperties.getTimePrecisionMode());
        }
        if (kafkaDataProperties.getDecimalHandlingMode() != null) {
            builder.withConfig("decimal.handling.mode", kafkaDataProperties.getDecimalHandlingMode());
        }

        addTransformRouter(task, builder);

        return builder.build();
    }

    /**
     * 构建JDBC连接字符串
     *
     * @param useProperties 是否添加连接属性
     * @return JDBC连接字符串
     */
    @Override
    public String getJdbcUrlString(boolean useProperties) {
        String jdbcUrl = JdbcUrlUtil.getUrl(connectionInfo.getType(), connectionInfo.getDbHost(), connectionInfo.getDbPort(), connectionInfo.getDbSchema(), connectionInfo.getExtConfig());
        if (useProperties) {
            jdbcUrl = super.addProperties(jdbcUrl);
        }
        return jdbcUrl;
    }

    /**
     * 构建Kafka Topic名称，格式：服务名.数据库名.表名
     *
     * @param taskId   数据同步任务ID
     * @param tableName 表名
     */
    @Override
    public String buildTopicName(Integer taskId, String tableName) {
        // Kafka Topic 名称只允许部分字符，非法表名用 MD5 摘要替代，避免超长 & 非法字符
        if (!isValidTopicName(tableName)) {
            String md5 = MD5Util.encode(tableName);
            tableName = md5 != null ? "T" + md5 : "T";
        }
        return String.format("%s.%s.%s", getServerName(taskId), connectionInfo.getDbSchema(), tableName);
    }

    /**
     * 构建目标表名，格式：数据库名.表名
     *
     * @param task      数据同步任务
     * @param tableName 表名
     * @return 目标表名
     */
    @Override
    public String buildFormatTargetTableName(DataSyncTask task, String tableName) {
        return getTargetTableFullPathString(task.getTargetDatabase().getConnectionInfo().getDbName(), tableName);
    }
}
