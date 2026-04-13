package com.pufferfishscheduler.cdc.kafka.database;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.cdc.kafka.RealTimeDataSyncEngineConfig;
import com.pufferfishscheduler.cdc.kafka.entity.DataSyncTask;
import com.pufferfishscheduler.cdc.kafka.entity.FieldMapper;
import com.pufferfishscheduler.cdc.kafka.entity.TableMapper;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * 数据源适配类
 */
@Data
public abstract class DataSourceAdapter {

    /**
     * 数据库类型
     */
    public enum DataBaseType {
        MySQL, Oracle, PostgreSQL, SQLServer, DM8, Doris
    }

    private DataBaseType type;// 数据库类型
    protected DBConnectionInfo connectionInfo;

    /**
     * 实时数据同步引擎配置
     */
    protected RealTimeDataSyncEngineConfig config;

    /**
     * AES工具类
     */
    protected AESUtil aesUtil;

    /**
     * 构造方法
     */
    public DataSourceAdapter() {
        this.config = PufferfishSchedulerApplicationContext.getBean(RealTimeDataSyncEngineConfig.class);
        this.aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);
    }

    /**
     * 清空表数据
     *
     * @param tables 表名列表
     */
    public abstract void truncateTables(List<String> tables);

    /**
     * 构建数据源连接器
     *
     * @param task 数据同步任务
     * @return 数据源连接器定义
     */
    public abstract NewConnectorDefinition buildSourceConnector(DataSyncTask task);

    /**
     * 获取url链接字符串
     *
     * @return
     */
    public abstract String getJdbcUrlString(boolean useProperties);

    /**
     * 获取kafka主题名称
     *
     * @param tableName 表名
     * @return kafka主题名称
     */
    public abstract String buildTopicName(Integer taskId, String tableName);

    /**
     * 构建格式化的目标表表名
     *
     * @return
     */
    public abstract String buildFormatTargetTableName(DataSyncTask task, String tableName);

    /**
     * 构建目标连接器配置
     * 参考地址：https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/sink_config_options.html
     *
     * @param task 数据同步任务
     * @return 目标连接器定义列表
     */
    public List<NewConnectorDefinition> buildTargetConnector(DataSyncTask task) {

        List<NewConnectorDefinition> result = new ArrayList<NewConnectorDefinition>();
        for (TableMapper tableMapper : task.getTableMappers()) {
            NewConnectorDefinition.Builder builder = NewConnectorDefinition.newBuilder()
                    .withName(getTargetConnectorName(task.getTaskId(), tableMapper.getTargetTableName()))
                    .withConfig("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");

            if (tableMapper.getParallelWriteFlag()) {
                builder.withConfig("tasks.max", tableMapper.getParallelThreadNum());
            } else {
                builder.withConfig("tasks.max", "1");
            }

            builder
                    .withConfig("topics",
                            task.getSourceDatabase().buildTopicName(task.getTaskId(), tableMapper.getSourceTableName()))
                    .withConfig("connection.url", getJdbcUrlString(true))
                    .withConfig("connection.user", this.connectionInfo.getUsername())
                    .withConfig("connection.password", aesUtil.decrypt(this.connectionInfo.getPassword()))
                    .withConfig("table.name.format", buildFormatTargetTableName(task, tableMapper.getTargetTableName()))
                    .withConfig("auto.evolve", "false")
                    .withConfig("auto.create", "false")
                    .withConfig("transforms", "unwrap")
                    .withConfig("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                    .withConfig("transforms.unwrap.drop.tombstones", "false");

            if (task.getSourceDatabase().isNeedContentRouter(task)) {
                builder.withConfig("transforms.unwrap.delete.handling.mode", "none");
            }

            // 仅插入
            if (DataSyncTask.WRITETYPE_ONLY_INSERT.equals(tableMapper.getWriteType())) {
                builder.withConfig("insert.mode", "insert")
                        .withConfig("transforms", "unwrap,RenameField2");
            } else {// 插入更新
                List<String> sourceKeys = tableMapper.getSourceKeys();
                if (sourceKeys != null && !sourceKeys.isEmpty()) {
                    builder
                            .withConfig("insert.mode", "upsert")
                            .withConfig("pk.fields", getTargetFormatFieldKeys(tableMapper))// 多个字段之间使用逗号隔开
                            .withConfig("pk.mode", "record_key")
                            .withConfig("transforms", "unwrap,RenameField1,RenameField2")
                            .withConfig("transforms.RenameField1.type", "org.apache.kafka.connect.transforms.ReplaceField$Key")
                            .withConfig("transforms.RenameField1.renames", getTargetFormatKeyMappersString(tableMapper))
                            .withConfig("delete.enabled", "true"); // null value as delete record
                } else {
                    throw new BusinessException(
                            String.format("源表[%s]不存在物理主键或者逻辑主键，无法写入方式不能为“插入/更新”模式！", tableMapper.getSourceTableName()));
                }
            }

            builder
                    .withConfig("transforms.RenameField2.type",
                            "org.apache.kafka.connect.transforms.ReplaceField$Value")
                    .withConfig("transforms.RenameField2.renames", getTargetFormatColumnMappersString(tableMapper))
                    .withConfig("source.database.type", task.getSourceDatabase().getType().name().toUpperCase())// 源数据库类型，在写入目标数据库时，需要根据此类型进行不同的日期格式转换
                    // .withConfig("db.timezone","Asia/Shanghai")//默认采用东八区。后面页面可以加入此时区配置
                    .withConfig("consumer.override.max.poll.records", config.consumerMaxPollRecords)
                    .withConfig("consumer.override.max.partition.fetch.bytes", config.consumerMaxPartitionFetchBytes)
                    .withConfig("consumer.override.fetch.min.bytes", config.consumerFetchMinBytes)
                    .withConfig("consumer.override.fetch.max.wait.ms", config.consumerFetchMaxWaitMs)
                    .withConfig("consumer.override.compression.type", config.consumerCompressionType)
                    .withConfig("batch.size", tableMapper.getBatchSize());

            result.add(builder.build());
        }

        return result;
    }

    /**
     * 为JDBC URL添加连接属性
     *
     * @param jdbcUrl 原始JDBC URL
     * @return 包含连接属性的JDBC URL
     */
    protected String addProperties(String jdbcUrl) {
        HashMap<String, String> hashMap = JSONObject.parseObject(connectionInfo.getProperties(), HashMap.class);

        Properties props = new Properties();

        // Schema 设置（按数据库类型适配）
        if (StringUtils.isNotBlank(connectionInfo.getDbSchema())) {
            if (Constants.DATABASE_TYPE.POSTGRESQL.equals(connectionInfo.getType())) {
                props.put("currentSchema", connectionInfo.getDbSchema());
            } else {
                props.put("schema", connectionInfo.getDbSchema());
            }
        }

        // mysql类型数据库设置默认连接参数 按照系统情况修改默认参数
        if (Constants.DATABASE_TYPE.MYSQL.equals(connectionInfo.getType())) {
            props.put("useCursorFetch", "true");
            props.put("useSSL", "false");
            // MySQL 8 + caching_sha2_password 认证常见必需项，否则会报 “Public Key Retrieval is not allowed”
            props.put("allowPublicKeyRetrieval", "true");
            props.put("useUnicode", "yes");
            props.put("characterEncoding", "UTF-8");
            props.put("zeroDateTimeBehavior", "convertToNull");
            props.put("allowMultiQueries", "true");
            props.put("serverTimezone", "Asia/Shanghai");
            props.put("useOldAliasMetadataBehavior", "true");
        }

        if (null != hashMap && !hashMap.isEmpty()) {
            props.putAll(hashMap);
        }

        if (Constants.DATABASE_TYPE.SQL_SERVER.equals(connectionInfo.getType())) {
            if (!props.isEmpty()) {
                StringBuilder jdbcUrlBuilder = new StringBuilder(jdbcUrl);
                for (String key : props.stringPropertyNames()) {
                    jdbcUrlBuilder.append(key).append("=").append(props.getProperty(key)).append(";");
                }
                jdbcUrl = jdbcUrlBuilder.toString();
            }
        } else {
            if (!props.isEmpty()) {
                StringBuilder jdbcUrlBuilder = new StringBuilder(jdbcUrl + "?");
                for (String key : props.stringPropertyNames()) {
                    jdbcUrlBuilder.append(key).append("=").append(props.getProperty(key)).append("&");
                }
                jdbcUrl = jdbcUrlBuilder.toString();
                jdbcUrl = jdbcUrl.substring(0, jdbcUrl.length() - 1);
            }
        }

        return jdbcUrl;
    }

    /**
     * 校验数据源配置
     */
    public void valid() {
        if (StringUtils.isEmpty(connectionInfo.getDbHost())) {
            throw new BusinessException("源库主机地址不能为空！");
        }

        if (connectionInfo.getDbPort() == null) {
            throw new BusinessException("源库端口号不能为空！");
        }

        if (StringUtils.isEmpty(connectionInfo.getDbName())) {
            throw new BusinessException("数据库不能为空！");
        }
    }

    /**
     * 获取源连接器名称
     *
     * @param taskId 任务ID
     * @return 源连接器名称
     */
    public String getSourceConnectorName(Integer taskId) {
        return String.format("_%s%s_", "S", taskId);
    }

    /**
     * 获取目标连接器名称
     *
     * @param taskId    任务ID
     * @param tableName 表名称
     * @return 目标连接器名称
     */
    public String getTargetConnectorName(Integer taskId, String tableName) {
        return String.format("%s%s_", getTargetConnectorNamePrefix(taskId), tableName);
    }

    /**
     * 获取目标连接器名称前缀
     *
     * @param taskId 任务ID
     * @return 目标连接器名称前缀
     */
    public String getTargetConnectorNamePrefix(Integer taskId) {
        return String.format("_%s%s_", "D", taskId);
    }


    /**
     * 获取服务器名称
     *
     * @param taskId 任务ID
     * @return 服务器名称
     */
    public String getServerName(Integer taskId) {
        return String.format("%s%s_", "SERVER", taskId);
    }

    /**
     * 获取历史主题名称
     *
     * @param taskId 任务ID
     * @param dbName 数据库名称
     * @return 历史主题名称
     */
    public String getHistoryTopicName(Integer taskId, String dbName) {
        return String.format("%s.%s", getServerName(taskId), dbName);
    }

    /**
     * 获取源表全路径字符串
     *
     * @param dbName       数据库名称
     * @param tableMappers 表映射关系列表
     * @return 源表全路径字符串
     */
    public String getSourceTableFullPathsString(String dbName, List<TableMapper> tableMappers) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < tableMappers.size(); i++) {
            sb.append(String.format("%s.%s", dbName, tableMappers.get(i).getSourceTableName()));

            if (i < tableMappers.size() - 1) {
                sb.append(",");
            }
        }

        return sb.toString();

    }

    /**
     * 获取源表字段映射字符串
     *
     * @param dbName       数据库名称
     * @param tableMappers 表映射关系列表
     * @return 源表字段映射字符串
     */
    public String getSourceColumnFullPathsString(String dbName, List<TableMapper> tableMappers) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < tableMappers.size(); i++) {
            TableMapper tableMapper = tableMappers.get(i);

            for (int j = 0; j < tableMapper.getFieldMappers().size(); j++) {
                sb.append(String.format("%s.%s.%s", dbName, tableMapper.getSourceTableName(),
                        tableMapper.getFieldMappers().get(j).getSourceFieldName()));
                sb.append(",");
            }
        }

        String result = sb.toString();
        result = result.substring(0, result.length() - 1);
        return result;
    }

    /**
     * 获取源表逻辑主键映射字符串
     *
     * @param dbName       数据库名称
     * @param tableMappers 表映射关系列表
     * @return 源表逻辑主键映射字符串
     */
    public static String getSourceKeyFullPathsString(String dbName, List<TableMapper> tableMappers) {

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < tableMappers.size(); i++) {
            TableMapper tableMapper = tableMappers.get(i);

            StringBuffer sb = new StringBuffer();
            boolean isFirst = true;
            for (int j = 0; j < tableMapper.getFieldMappers().size(); j++) {
                FieldMapper fieldMapper = tableMapper.getFieldMappers().get(j);

                if (fieldMapper.getIsLogicKey()) {
                    if (isFirst) {
                        sb.append(String.format("%s.%s:", dbName, tableMapper.getSourceTableName()));
                        isFirst = false;
                    }
                    sb.append(fieldMapper.getSourceFieldName());
                    sb.append(",");
                }
            }

            String sTemp = sb.toString();
            if (StringUtils.isNotEmpty(sTemp)) {
                result.append(sTemp.substring(0, sTemp.length() - 1));
                result.append(";");
            }
        }

        String sResult = result.toString();
        if (StringUtils.isNotEmpty(sResult)) {
            sResult = sResult.substring(0, sResult.length() - 1);
        }

        return sResult;
    }

    /**
     * 获取源表逻辑主键映射字符串
     *
     * @param keys 逻辑主键字段名称列表
     * @return 源表逻辑主键映射字符串
     */
    public String getSourceFormatFieldKeys(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        for (String key : keys) {
            sb.append(key).append(",");
        }

        String result = sb.toString();

        return result.substring(0, result.length() - 1);
    }

    /**
     * 获取目标表逻辑主键映射字符串
     *
     * @param tableMapper 表映射关系
     * @return 目标表逻辑主键映射字符串
     */
    public String getTargetFormatFieldKeys(TableMapper tableMapper) {
        StringBuffer sb = new StringBuffer();
        for (FieldMapper fieldMapper : tableMapper.getFieldMappers()) {
            if (fieldMapper.getIsLogicKey()) {
                sb.append(fieldMapper.getTargetFieldName()).append(",");
            }
        }

        String result = sb.toString();
        if (StringUtils.isNotEmpty(result)) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }

    /**
     * 获取目标表逻辑主键映射字符串
     *
     * @param tableMapper 表映射关系
     * @return 目标表逻辑主键映射字符串
     */
    public String getTargetFormatKeyMappersString(TableMapper tableMapper) {
        StringBuffer sb = new StringBuffer();
        for (FieldMapper fieldMapper : tableMapper.getFieldMappers()) {
            if (fieldMapper.getIsLogicKey()) {
                sb.append(fieldMapper.getSourceFieldName()).append(":").append(fieldMapper.getTargetFieldName())
                        .append(",");
            }
        }

        String result = sb.toString();
        if (StringUtils.isNotEmpty(result)) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }

    /**
     * 获取目标表字段映射字符串
     *
     * @param tableMapper 表映射关系
     * @return 目标表字段映射字符串
     */
    public String getTargetFormatColumnMappersString(TableMapper tableMapper) {
        StringBuffer sb = new StringBuffer();
        for (FieldMapper fieldMapper : tableMapper.getFieldMappers()) {
            sb.append(fieldMapper.getSourceFieldName()).append(":").append(fieldMapper.getTargetFieldName())
                    .append(",");
        }

        String result = sb.toString();
        if (StringUtils.isNotEmpty(result)) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }

    /**
     * 获取目标表完整路径字符串
     *
     * @param dbName    数据库名称
     * @param tableName 表名称
     * @return 目标表完整路径字符串
     */
    public String getTargetTableFullPathString(String dbName, String tableName) {
        return String.format("%s.%s", dbName, tableName);
    }

    // 添加内容路由
    protected void addTransformRouter(DataSyncTask task, NewConnectorDefinition.Builder builder) {
        if (!isNeedContentRouter(task)) {
            return;
        }

        builder.withConfig("transforms", "route")
                .withConfig("transforms.route.type", "io.debezium.transforms.ContentBasedRouter")
                .withConfig("transforms.route.language", "jsr223.groovy")
                .withConfig("schema.name.adjustment.mode", "none")
                .withConfig("include.schema.changes", "false");

        StringBuffer express = new StringBuffer();

        // express.append("if(value.op == null || value.op ==''){return null;}/n");
        for (TableMapper tm : task.getTableMappers()) {
            express.append("if(value.source.table == ").append("'").append(tm.getSourceTableName()).append("'){")
                    .append("return '").append(buildTopicName(task.getTaskId(), tm.getSourceTableName()))
                    .append("';}\n");
        }

        builder.withConfig("transforms.route.topic.expression", express.toString());
    }

    /**
     * 是否需要按照kafka内容进行topic 路由
     *
     * @param task
     * @return
     */
    public boolean isNeedContentRouter(DataSyncTask task) {
        List<TableMapper> list = task.getTableMappers();
        if (list == null || list.isEmpty()) {
            return false;
        }

        for (TableMapper tm : list) {
            if (!isValidTopicName(tm.getSourceTableName())) {
                return true;
            }
        }

        return false;
    }

    /**
     * 校验topic名称是否合法
     *
     * @param topicName topic名称
     * @return 是否合法
     */
    protected boolean isValidTopicName(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return false;
        }

        for (int i = 0; i < topicName.length(); i++) {
            char c = topicName.charAt(i);
            if (!isValidTopicNameCharacter(c)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Whether the given character is a legal character of a Kafka topic name. Legal
     * characters are
     * {@code [a-zA-Z0-9._-]}.
     *
     * @link https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java
     */
    private boolean isValidTopicNameCharacter(char c) {
        return c == '.' || c == '_' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9');
    }
}
