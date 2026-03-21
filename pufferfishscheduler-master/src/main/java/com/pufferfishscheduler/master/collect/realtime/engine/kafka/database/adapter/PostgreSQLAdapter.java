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
 * PostgreSQL 数据库适配器
 *
 * @author Mayc
 * @since 2026-03-16 12:49
 */
@Slf4j
public class PostgreSQLAdapter extends DataSourceAdapter {

    /**
     * 清理目标表数据
     *
     * @param tables 表名列表
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
            log.error("清理 PostgreSQL 目标表数据失败", e);
            throw new BusinessException(e.getMessage());
        }
    }

    /**
     * 构建源端连接器配置
     *
     * @param task 数据同步任务
     * @return 源端连接器配置
     */
    @Override
    public NewConnectorDefinition buildSourceConnector(DataSyncTask task) {
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);
        KafkaDataProperties kafkaDataProperties = PufferfishSchedulerApplicationContext.getBean(KafkaDataProperties.class);

        NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()
                .withName(getSourceConnectorName(task.getTaskId()))
                .withConfig("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .withConfig("tasks.max", "1");

        // 源库配置
        builder.withConfig("database.hostname", task.getSourceDatabase().getConnectionInfo().getDbHost())
                .withConfig("database.port", task.getSourceDatabase().getConnectionInfo().getDbPort())
                .withConfig("database.user", task.getSourceDatabase().getConnectionInfo().getUsername())
                .withConfig("database.password", aesUtil.decrypt(connectionInfo.getPassword()))
                .withConfig("database.server.id", task.getTaskId())
                .withConfig("database.server.name", getServerName(task.getTaskId()))
                .withConfig("database.dbname", task.getSourceDatabase().getConnectionInfo().getDbName())
                .withConfig("schema.include.list", task.getSourceDatabase().getConnectionInfo().getDbSchema())
                .withConfig("snapshot.mode", "initial");

        if (DataSyncTask.DATA_SYNC_TYPE_INCREMENT.equals(task.getDataSyncType())) {
            builder.withConfig("snapshot.mode", "never");
        }

        String dbName = task.getSourceDatabase().getConnectionInfo().getDbName();
        String schema = task.getSourceDatabase().getConnectionInfo().getDbSchema();
        String keys = getSourceKeyFullPathsString(dbName, task.getTableMappers());
        if (StringUtils.isNotEmpty(keys)) {
            builder.withConfig("message.key.columns", keys);
        }

        builder.withConfig("tombstones.on.delete", "true")
                .withConfig("topic.prefix", getServerName(task.getTaskId()));

        if (Boolean.TRUE.equals(task.getHeartbeatEnabled())) {
            String interval = task.getHeartbeatInterval() != null
                    ? task.getHeartbeatInterval().toString()
                    : "3600000";
            builder.withConfig("heartbeat.interval.ms", interval);
        }

        // 表/字段清单、Kafka 历史、PostgreSQL 逻辑复制
        builder.withConfig("table.include.list", getSourceTableFullPathsString(schema, task.getTableMappers()))
                .withConfig("column.include.list", getSourceColumnFullPathsString(schema, task.getTableMappers()))
                .withConfig("database.history.kafka.bootstrap.servers", task.getBrokers())
                .withConfig("database.history.kafka.topic", getHistoryTopicName(task.getTaskId(), dbName))
                .withConfig("publication.autocreate.mode", "disabled")
                .withConfig("slot.name", getSlotName(task.getTaskId()))
                .withConfig("plugin.name", "pgoutput")
                .withConfig("publication.name", getPublicationName(task.getTaskId()))
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
     * 构建 Debezium 槽名，格式：slot + 任务ID
     */
    private static String getSlotName(Integer taskId) {
        return "slot" + taskId;
    }

    /**
     * 构建 Debezium 发布名，格式：lsdpub + 任务ID
     */
    private static String getPublicationName(Integer taskId) {
        return "lsdpub" + taskId;
    }

    /**
     * 构建 JDBC URL 字符串，根据是否使用属性配置进行加密处理
     * @param useProperties 是否使用属性配置
     * @return JDBC URL 字符串
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
     * 构建 Kafka Topic 名称，格式为：{服务名}.{数据库名}.{表名}
     * @param taskId    任务ID
     * @param tableName 表名
     * @return Kafka Topic 名称
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
     * 构建目标表的完整路径，格式为：数据库名.表名
     * @param task      数据同步任务
     * @param tableName 表名
     * @return 目标表的完整路径
     */
    @Override
    public String buildFormatTargetTableName(DataSyncTask task, String tableName) {
        return getTargetTableFullPathString(task.getTargetDatabase().getConnectionInfo().getDbSchema(), tableName);
    }
}
