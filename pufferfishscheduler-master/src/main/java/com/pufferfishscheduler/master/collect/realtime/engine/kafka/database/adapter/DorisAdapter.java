package com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.adapter;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.DataSourceAdapter;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.DataSyncTask;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.FieldMapper;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.TableMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Doris 数据库适配器
 *
 * @author Mayc
 * @since 2026-03-16  16:28
 */
@Slf4j
public class DorisAdapter extends DataSourceAdapter {

    private static final String QUERY_MODEL_SQL_TEMPLATE = "SHOW CREATE TABLE `%s`.`%s`";

    /**
     * 清空目标表数据
     *
     * @param tables 表名列表
     */
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

    /**
     * 构建源连接器定义
     *
     * @param task 数据同步任务
     * @return 源连接器定义
     */
    @Override
    public NewConnectorDefinition buildSourceConnector(DataSyncTask task) {
        throw new BusinessException("暂不支持Doris数据源建立源连接器！");
    }

    @Override
    public List<NewConnectorDefinition> buildTargetConnector(DataSyncTask task) {
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);

        String extConfigStr = connectionInfo.getExtConfig();
        if (extConfigStr == null || "".equals(extConfigStr.trim())) {
            throw new BusinessException("需要配置FE地址！");
        }

        JSONObject jo = JSONObject.parseObject(extConfigStr);
        String fe = jo.getString("feAddress");

        if (fe == null || "".equals(fe.trim())) {
            throw new BusinessException("FE地址不能为空！");
        }


        /**
         * 参考地址：https://doris.apache.org/zh-CN/docs/ecosystem/doris-kafka-connector
         */

        List<NewConnectorDefinition> result = new ArrayList<NewConnectorDefinition>();
        for (TableMapper tableMapper : task.getTableMappers()) {
            NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()
                    .withName(getTargetConnectorName(task.getTaskId(), tableMapper.getTargetTableName()))
                    // doris连接器
                    .withConfig("connector.class", "org.apache.doris.kafka.connector.DorisSinkConnector");

            // 最大任务数
            if (tableMapper.getParallelWriteFlag()) {
                builder.withConfig("tasks.max", tableMapper.getParallelThreadNum());
            } else {
                builder.withConfig("tasks.max", "1");
            }

            // doris配置
            String topicName = task.getSourceDatabase().buildTopicName(task.getTaskId(), tableMapper.getSourceTableName());
            builder
                    .withConfig("topics", topicName)
                    .withConfig("doris.topic2table.map", topicName + ":" + tableMapper.getTargetTableName())
                    // Doris FE 连接地址。如果有多个，中间用逗号分割
                    .withConfig("doris.urls", task.getTargetDatabase().getConnectionInfo().getDbHost())
                    .withConfig("doris.user", task.getTargetDatabase().getConnectionInfo().getUsername())
                    .withConfig("doris.password", aesUtil.decrypt(connectionInfo.getPassword()))
                    // 	Doris HTTP 协议端口
                    .withConfig("doris.http.port", getFeAddressPort(fe))
                    // 	Doris MySQL 协议端口
                    .withConfig("doris.query.port", task.getTargetDatabase().getConnectionInfo().getDbPort())
                    .withConfig("doris.database", task.getTargetDatabase().getConnectionInfo().getDbName())
                    // 消费 Kafka 数据时，上游数据的类型转换模式   normal  debezium_ingestion
                    .withConfig("converter.mode", "debezium_ingestion")
                    // 是否同步删除记录，默认false
                    .withConfig("enable.delete", "true")
                    .withConfig("enable.combine.flush", "true")
                    // 在 flush 到 doris 之前，每个 Kafka 分区在内存中缓冲的记录数 默认10000条
                    .withConfig("buffer.count.records", "20")
                    // buffer 刷新间隔，单位秒，默认 120 秒
                    .withConfig("buffer.flush.time", "1")
//                    // 	每个 Kafka 分区在内存中缓冲的记录的累积大小，单位字节，默认5MB
//                    .withConfig("buffer.size.bytes", "5000000")
//                    // 时区
//                    .withConfig("database.time_zone", "Asia/Shanghai")
//                    // 缓冲区刷新间隔
//                    .withConfig("bufferflush.intervalms", "30000")
//                    // 连接超时时间
//                    .withConfig("connect.timeoutms", "5000")
            ;

            // 数据源名称
            String dbName = task.getTargetDatabase().getConnectionInfo().getDbName();

            // 插入更新
            if (DataSyncTask.WRITETYPE_INSERT_UPDATE.equals(tableMapper.getWriteType())) {
                List<String> sourceKeys = tableMapper.getSourceKeys();
                if (sourceKeys != null && sourceKeys.size() > 0) {

                    builder
                            // 指明主键
                            .withConfig("sink.properties.primary_key", getTargetFormatFieldKeys(tableMapper))
                            // 部分列更新
                            .withConfig("sink.properties.partial_columns", "true")
                            // 更新指定列
                            .withConfig("sink.properties.columns", getTargetColumns(tableMapper, dbName))
                    ;
                } else {
                    throw new BusinessException(String.format("源表[%s]不存在物理主键或者逻辑主键，无法写入方式不能为“插入/更新”模式！", tableMapper.getSourceTableName()));
                }
            }

            // 格式化字段映射关系
//            String columnMappers = getTargetFormatColumnMappersString(tableMapper);

            // 如果是主键的话
            if (isPrimaryTable(dbName, tableMapper.getTargetTableName())) {

//                columnMappers = columnMappers + ",__DORIS_DELETE_SIGN__:__DORIS_DELETE_SIGN__";

                // 指定了 Kafka Connect 应用的数据转换链（Transforms） unwrap 用于解包或转换嵌套的结构; RenameField2 用于重命名字段
//                builder
//                        .withConfig("transforms", "unwrap,RenameField2")
//                        // 针对 unwrap 转换器的配置,控制如何处理删除操作的消息  rewrite 删除的记录会被保留，但它们的内容将被重写为空值
//                        .withConfig("transforms.unwrap.delete.handling.mode", "rewrite")
//                        // 使用 Kafka Connect 的 ReplaceField 转换器来对消息的 值（value）部分进行字段重命名
//                        .withConfig("transforms.RenameField2.type", "org.apache.kafka.connect.transforms.ReplaceField$Value")
//                        // 重命名字段
//                        .withConfig("transforms.RenameField2.renames", columnMappers)
//                        // Debezium 提供的一个转换器，用于从 Debezium 的变更数据捕获（CDC）消息中提取出 "新记录"
//                        .withConfig("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
//                        // 是否丢弃 "墓碑"（tombstone）消息。在 Debezium 中，"墓碑" 消息代表删除操作的标志，通常用于标示数据删除
//                        .withConfig("transforms.unwrap.drop.tombstones", "false")

//                        // 是否同步删除记录，默认false
//                        .withConfig("enable.delete", "true")
                // 开启schema权限，doris删除会在目标表创建 doris_delete_mark 标记字段
//                        .withConfig("debezium.schema.evolution", "basic")
                ;

            }

//            builder
//                    // 消费者每次调用 poll 时能够获取的最大记录数
//                    .withConfig("consumer.override.max.poll.records", config.consumerMaxPollRecords)
//                    // 每个 Kafka 分区一次拉取的最大字节数
//                    .withConfig("consumer.override.max.partition.fetch.bytes", config.consumerMaxPartitionFetchBytes)
//                    // 指定每次从 Kafka 拉取数据时，最小的字节数
//                    .withConfig("consumer.override.fetch.min.bytes", config.consumerFetchMinBytes)
//                    // 消费者在发出拉取请求后，等待数据返回的最大时间（毫秒）
//                    .withConfig("consumer.override.fetch.max.wait.ms", config.consumerFetchMaxWaitMs)
//                    // 指定消费者解压消息时使用的压缩类型
//                    .withConfig("consumer.override.compression.type", config.consumerCompressionType)
//                    // 是否移除 JSON 数据的外部数组包装
////                    .withConfig("sink.properties.strip_outer_array", "true")
////                    .withConfig("sink.properties.strip_outer_array", "false")
//                    // 每个批次记录数的大小
//                    .withConfig("batch.size", tableMapper.getBatchSize());

            // 处理 key/value 转换，Debezium ingestion 模式要求使用 JsonConverter 并保留 schema
            builder
                    .withConfig("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                    .withConfig("key.converter.schemas.enable", "true")
                    .withConfig("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                    .withConfig("value.converter.schemas.enable", "true");

            result.add(builder.build());
        }

        return result;
    }


    /**
     * 构建 JDBC URL 字符串
     *
     * @param useProperties 是否添加连接属性
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
     * 构建 Kafka Topic 名称
     *
     * @param taskId    数据同步任务ID
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

    /**
     * 判断是否为主键表
     *
     * @param schema
     * @param tableName
     * @return
     */
    public boolean isPrimaryTable(String schema, String tableName) {

        String sql = String.format(QUERY_MODEL_SQL_TEMPLATE, schema, tableName);

        Connection conn = null;
        PreparedStatement stats = null;
        ResultSet rs = null;
        try {
            conn = JdbcUtil.getConnection(JdbcUtil.getDriver(connectionInfo.getType()), getJdbcUrlString(false), connectionInfo);

            log.info(String.format("执行SQL: %s", sql));
            stats = conn.prepareStatement(sql);

            rs = stats.executeQuery();
            if (rs.next()) {
                String createTableSql = rs.getString(2);
                // 检查建表语句中是否包含主键定义
                if (createTableSql != null &&
                        (createTableSql.contains("PRIMARY KEY") ||
                                createTableSql.contains("UNIQUE KEY") ||
                                createTableSql.contains("ENGINE=OLAP") && createTableSql.contains("DUPLICATE KEY"))) {
                    return true;
                }
            }

            return false;

        } catch (Exception e) {
            log.error("", e);
            throw new BusinessException(e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stats);
            JdbcUtil.close(conn);
        }

    }

    /**
     * 获取目标表字段
     *
     * @param tableMapper 表映射器
     * @param dbName      数据库名
     * @return 目标表字段字符串
     */
    private String getTargetColumns(TableMapper tableMapper, String dbName) {
        if (tableMapper == null || tableMapper.getFieldMappers() == null || StringUtils.isBlank(dbName)) {
            return "";
        }

        List<FieldMapper> fieldMappers = tableMapper.getFieldMappers();
        if (fieldMappers.isEmpty()) {
            return "";
        }

        StringBuilder columns = new StringBuilder();
        Iterator<FieldMapper> iterator = fieldMappers.iterator();

        // 先添加第一个元素，避免后续判断
        columns.append(iterator.next().getTargetFieldName());

        // 从第二个元素开始遍历，前面加逗号
        while (iterator.hasNext()) {
            columns.append(",").append(iterator.next().getTargetFieldName());
        }

        return columns.toString();
    }

    /**
     * 获取FE地址端口
     *
     * @param feAddress
     * @return
     */
    private String getFeAddressPort(String feAddress) {

        if (StringUtils.isBlank(feAddress)) {
            throw new BusinessException("请校验 Doris 数据源是否配置FE地址信息!");
        }

        if (!feAddress.contains(":")) {
            throw new BusinessException("请校验 Doris 数据源FE地址是否配置端口号!");
        }

        feAddress = feAddress.replace("http://", "").replace("https://", "");
        String[] split = feAddress.split(":");
        return split[1];
    }
}
