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
 * SQLServer 数据库适配器，继承 PostgreSQL 适配器。
 *
 * @author Mayc
 * @since 2026-03-16  13:08
 */
@Slf4j
public class SQLServerAdapter extends DataSourceAdapter {

    private static final String DEFAULT_SCHEMA_NAME = "dbo";

    @Override
    public void truncateTables(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }

        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(JdbcUtil.getDriver(connectionInfo.getType()), getJdbcUrlString(false), connectionInfo);

            for (String table : tables) {

                PreparedStatement stats = null;
                try {

                    String sql = String.format("truncate table [%s].[%s]", StringUtils.isNotBlank(this.connectionInfo.getDbSchema()) ? this.connectionInfo.getDbSchema() : DEFAULT_SCHEMA_NAME, table);

                    log.info(String.format("清理目标【%s】表数据！SQL: %s", table, sql));
                    stats = conn.prepareStatement(sql);
                    stats.executeUpdate();
                } catch (Exception e) {
                    log.error("", e);
                    throw new BusinessException(e.getMessage());
                } finally {
                    JdbcUtil.close(stats);
                }
            }

        } catch (Exception e) {
            log.error("", e);
            throw new BusinessException(e.getMessage());
        } finally {
            JdbcUtil.close(conn);
        }
    }

    @Override
    public NewConnectorDefinition buildSourceConnector(DataSyncTask task) {
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);
        KafkaDataProperties kafkaDataProperties = PufferfishSchedulerApplicationContext.getBean(KafkaDataProperties.class);

        //如果不传入schema，那么使用系统默认的schema
        if (task.getSourceDatabase().getConnectionInfo().getDbSchema() == null || task.getSourceDatabase().getConnectionInfo().getDbSchema().trim().isEmpty()) {
            task.getSourceDatabase().getConnectionInfo().setDbSchema(DEFAULT_SCHEMA_NAME);
        }

        NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()

                /**
                 * 使用首字母S+任务ID
                 */.withName(getSourceConnectorName(task.getTaskId())).withConfig("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .withConfig("tasks.max", "1");

        /**
         * 源库配置
         */
        builder.withConfig("database.hostname", task.getSourceDatabase().getConnectionInfo().getDbHost())//数据库连接
                .withConfig("database.port", task.getSourceDatabase().getConnectionInfo().getDbPort())
                .withConfig("database.user", task.getSourceDatabase().getConnectionInfo().getUsername())
                .withConfig("database.password", aesUtil.decrypt(connectionInfo.getPassword()))
                /**
                 * 实时任务的主键（数字类型）。只在新增的时候添加，不能修改。使用任务id作为服务id
                 */.withConfig("database.server.id", task.getTaskId())
                /**
                 * 实时任务的UUID值。只在新增的时候添加，不能修改。使用“SERVER_” + 任务ID
                 */.withConfig("database.server.name", getServerName(task.getTaskId())).withConfig("database.dbname", task.getSourceDatabase().getConnectionInfo().getDbName())//同步的数据库
                .withConfig("schema.include.list", task.getSourceDatabase().getConnectionInfo().getDbSchema())
                /**
                 * initial （全量 + 增量）- 当database.server.name 对应的服务，没有记录日志读取的offset，那么执行快照同步 （the connector runs a snapshot only when no offsets have been recorded for the logical server name）
                 * never （增量） - the connector never uses snapshots. Upon first startup with a logical server name, the connector reads from the beginning of the binlog. Configure this behavior with care. It is valid only when the binlog is guaranteed to contain the entire history of the database.
                 *
                 */.withConfig("snapshot.mode", "initial");

        if (DataSyncTask.DATA_SYNC_TYPE_INCREMENT.equals(task.getDataSyncType())) {
            builder.withConfig("snapshot.mode", "schema_only");
        }

        String keys = getSourceKeyFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbSchema(), task.getTableMappers());

        if (StringUtils.isNotEmpty(keys)) {
            builder.withConfig("message.key.columns", keys);
        }
        builder.withConfig("tombstones.on.delete", "true").withConfig("topic.prefix", getServerName(task.getTaskId()));

        if (null != task.getHeartbeatEnabled() && task.getHeartbeatEnabled()) {
            builder.withConfig("heartbeat.interval.ms", null != task.getHeartbeatInterval() ? task.getHeartbeatInterval().toString() : "3600000");
        }


        /**
         * 需要同步的表清单。dbname1.tablename1 , dbname1.tablename2  ......
         */
        builder.withConfig("table.include.list", getSourceTableFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbSchema(), task.getTableMappers()))

                /**
                 * 需要同步表的主键，多个key之间使用逗号隔开，debezium在做数据更新时候会用到此key。<fully-qualified_tableName>:_<keyColumn>_,<keyColumn>
                 */
                /**
                 * 需要同步的字段列表，多个字段之间使用逗号隔开。databaseName.tableName.columnName
                 */
                .withConfig("column.include.list", getSourceColumnFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbSchema(), task.getTableMappers()))

                /**
                 * A list of host/port pairs that the connector uses for establishing an initial connection to the Kafka cluster. This connection is used for retrieving the database schema history previously stored by the connector, and for writing each DDL statement read from the source database. Each pair should point to the same Kafka cluster used by the Kafka Connect process
                 */
                .withConfig("database.history.kafka.bootstrap.servers", task.getBrokers())
                /**
                 * 格式：服务名.数据库名。The full name of the Kafka topic where the connector stores the database schema history.
                 */
                .withConfig("database.history.kafka.topic", getHistoryTopicName(task.getTaskId(), task.getSourceDatabase().getConnectionInfo().getDbName()))
                /**
                 * 性能优化参数
                 */
                .withConfig("connector.client.config.override.policy", "all")
                .withConfig("topic.creation.default.replication.factor", config.topicReplicationFactor)
                .withConfig("topic.creation.default.partitions", config.topicPartitions)
                .withConfig("topic.creation.default.compression.type", config.topicCompressionType)
                .withConfig("topic.creation.default.compression.type", config.topicCompressionType)
                .withConfig("producer.override.compression.acks", config.topicAcks)
                .withConfig("database.history.skip.unparseable.ddl", "true")
                .withConfig("database.history.store.only.monitored.tables.ddl", "true");



        if (null != kafkaDataProperties.getTimePrecisionMode()) {
            builder.withConfig("time.precision.mode", kafkaDataProperties.getTimePrecisionMode());
        }
        if (null != kafkaDataProperties.getDecimalHandlingMode()) {
            builder.withConfig("decimal.handling.mode", kafkaDataProperties.getDecimalHandlingMode());
        }

        addTransformRouter(task, builder);

        return builder.build();
    }

    /**
     * 构建JDBC连接字符串，格式：jdbc:sqlserver://host:port;databaseName=dbname;user=username;password=password
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
     * 构建目标主题名，格式：[服务名].[数据库].[表名]
     *
     * @param taskId   任务ID
     * @param tableName 表名
     * @return 格式化后的主题名
     */
    @Override
    public String buildTopicName(Integer taskId, String tableName) {
        if (!isValidTopicName(tableName)) {
            String md5 = MD5Util.encode(tableName);
            tableName = md5 != null ? "T" + md5 : "T";
        }

        String schemaName = StringUtils.isNotBlank(this.connectionInfo.getDbSchema()) ? this.connectionInfo.getDbSchema() : DEFAULT_SCHEMA_NAME;

        return String.format("%s.%s.%s", getServerName(taskId), schemaName, tableName);
    }

    /**
     * 构建目标表名，格式为：[数据库].[模式].[表名]。
     * <p>
     * 如果模式为空，则默认使用 dbo 模式。
     *
     * @param task      数据同步任务
     * @param tableName 表名
     * @return 格式化后的目标表名
     */
    @Override
    public String buildFormatTargetTableName(DataSyncTask task, String tableName) {
        if (task.getTargetDatabase().getConnectionInfo().getDbSchema() == null && "".equals(task.getTargetDatabase().getConnectionInfo().getDbSchema().trim())) {
            task.getTargetDatabase().getConnectionInfo().setDbSchema(DEFAULT_SCHEMA_NAME);
        }

        return String.format("%s.%s.%s", task.getTargetDatabase().getConnectionInfo().getDbName(), task.getTargetDatabase().getConnectionInfo().getDbSchema(), tableName);
    }
}
