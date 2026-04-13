package com.pufferfishscheduler.cdc.kafka.database.adapter;

import com.pufferfishscheduler.cdc.kafka.config.KafkaDataProperties;
import com.pufferfishscheduler.cdc.kafka.database.DataSourceAdapter;
import com.pufferfishscheduler.cdc.kafka.entity.DataSyncTask;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * MySQL 数据库适配器
 *
 * @author Mayc
 * @since 2026-03-16  12:01
 */
@Slf4j
public class MySQLAdapter extends DataSourceAdapter {

    @Override
    public void truncateTables(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return;
        }

        Connection conn = null;
        try {
            // 连接信息里的 password 通常是加密串，JDBC 直连需要明文密码
            DBConnectionInfo plainConn = new DBConnectionInfo();
            plainConn.setType(connectionInfo.getType());
            plainConn.setDbHost(connectionInfo.getDbHost());
            plainConn.setDbPort(connectionInfo.getDbPort());
            plainConn.setDbName(connectionInfo.getDbName());
            plainConn.setDbSchema(connectionInfo.getDbSchema());
            plainConn.setUsername(connectionInfo.getUsername());
            plainConn.setPassword(aesUtil.decrypt(connectionInfo.getPassword()));
            plainConn.setExtConfig(connectionInfo.getExtConfig());
            plainConn.setProperties(connectionInfo.getProperties());

            // 需要带上 MySQL 默认连接属性（如 allowPublicKeyRetrieval），否则 MySQL8 可能报 “Public Key Retrieval is not allowed”
            conn = JdbcUtil.getConnection(JdbcUtil.getDriver(connectionInfo.getType()), getJdbcUrlString(true), plainConn);

            for (String table : tables) {

                PreparedStatement stats = null;
                try {
                    String sql = String.format("truncate table `%s`", table);
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
        NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()

                /**
                 * 使用首字母S+任务ID
                 */
                .withName(getSourceConnectorName(task.getTaskId()))
                .withConfig("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .withConfig("tasks.max", "1");

        /**
         * 源库配置
         */
        builder
                .withConfig("database.hostname", task.getSourceDatabase().getConnectionInfo().getDbHost())//数据库连接
                .withConfig("database.port", task.getSourceDatabase().getConnectionInfo().getDbPort())
                .withConfig("database.user", task.getSourceDatabase().getConnectionInfo().getUsername())
                .withConfig("database.password", aesUtil.decrypt(connectionInfo.getPassword()))
                /**
                 * 实时任务的主键（数字类型）。只在新增的时候添加，不能修改。使用任务id作为服务id
                 */
                .withConfig("database.server.id", task.getTaskId())
                /**
                 * 实时任务的UUID值。只在新增的时候添加，不能修改。使用“SERVER_” + 任务ID
                 */
                .withConfig("database.server.name", getServerName(task.getTaskId()))
                .withConfig("database.include.list", task.getSourceDatabase().getConnectionInfo().getDbName())//同步的数据库
                /**
                 * initial （全量 + 增量）- 当database.server.name 对应的服务，没有记录日志读取的offset，那么执行快照同步 （the connector runs a snapshot only when no offsets have been recorded for the logical server name）
                 * never （增量） - the connector never uses snapshots. Upon first startup with a logical server name, the connector reads from the beginning of the binlog. Configure this behavior with care. It is valid only when the binlog is guaranteed to contain the entire history of the database.
                 * schema_only_recovery 当你确定数据库结构没变，只是想跳过历史检查让它直接从当前 Binlog 位点开始时使用
                 */
                .withConfig("snapshot.mode", "initial");

        if (DataSyncTask.DATA_SYNC_TYPE_INCREMENT.equals(task.getDataSyncType())) {
            builder.withConfig("snapshot.mode", "never");
        }

        String keys = getSourceKeyFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbName(), task.getTableMappers());
        if (StringUtils.isNotEmpty(keys)) {
            builder.withConfig("message.key.columns", keys);
        }

        builder.withConfig("tombstones.on.delete", "true")
                .withConfig("topic.prefix", getServerName(task.getTaskId()));
        ;
        if (null != task.getHeartbeatEnabled() && task.getHeartbeatEnabled()) {
            //启用心跳机制，没有填写心跳间隔，默认一小时
            builder.withConfig("heartbeat.interval.ms", null != task.getHeartbeatInterval() ? task.getHeartbeatInterval().toString() : "3600000");
        }

        /**
         * 需要同步的表清单。dbname1.tablename1 , dbname1.tablename2  ......
         */
        builder
                .withConfig("table.include.list", getSourceTableFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbName(), task.getTableMappers()))

                /**
                 * 需要同步表的主键，多个key之间使用逗号隔开，debezium在做数据更新时候会用到此key。<fully-qualified_tableName>:_<keyColumn>_,<keyColumn>
                 * 需要同步的字段列表，多个字段之间使用逗号隔开。databaseName.tableName.columnName
                 */
                .withConfig("column.include.list", getSourceColumnFullPathsString(task.getSourceDatabase().getConnectionInfo().getDbName(), task.getTableMappers()))

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
                .withConfig("producer.override.compression.acks", config.topicAcks)
                .withConfig("database.history.skip.unparseable.ddl", "true")
                .withConfig("database.history.store.only.monitored.tables.ddl", "true");

        // 增加重试频率和时长
        builder.withConfig("database.history.kafka.recovery.poll.interval.ms", "5000") // 每5秒找一次
                .withConfig("database.history.kafka.recovery.attempts", "5");          // 最多找5次

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
     * 构建JDBC连接字符串，格式：jdbc:mysql://host:port/database?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
     *
     * @param useProperties 是否添加连接属性
     * @return JDBC连接字符串
     */
    @Override
    public String getJdbcUrlString(boolean useProperties) {
        String jdbcUrl = JdbcUrlUtil.getUrl(connectionInfo.getType(), connectionInfo.getDbHost(), connectionInfo.getDbPort(), connectionInfo.getDbName(), connectionInfo.getExtConfig());
        if (useProperties) {
            jdbcUrl = super.addProperties(jdbcUrl);
        }
        return jdbcUrl;
    }

    /**
     * 构建Kafka Topic名称，格式：服务名.数据库名.表名
     *
     * @param taskId    数据同步任务ID
     * @param tableName 表名
     */
    @Override
    public String buildTopicName(Integer taskId, String tableName) {
        // Kafka Topic 名称只允许部分字符，非法表名用 MD5 摘要替代，避免超长 & 非法字符
        if (!isValidTopicName(tableName)) {
            String md5 = MD5Util.encode(tableName);
            tableName = md5 != null ? "T" + md5 : "T";
        }
        return String.format("%s.%s.%s", getServerName(taskId), connectionInfo.getDbName(), tableName);
    }

    /**
     * 构建目标表名，格式：数据库名.表名
     *
     * @param task
     * @param tableName
     * @return
     */
    @Override
    public String buildFormatTargetTableName(DataSyncTask task, String tableName) {
        return getTargetTableFullPathString(task.getTargetDatabase().getConnectionInfo().getDbName(), tableName);
    }

}
