package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.ListenDatabaseType;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.vo.collect.FieldMappingVo;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 表输出组件元数据构造器
 * 
 * @author pufferfish
 */
@Component
public class TableOutputConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取数据库服务和AES工具
        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);

        PluginRegistry registryID = context.getRegistryID();
        Map<String, StepMeta> map = context.getStepMetaMap();
        String id = context.getId();
        Integer flowId = context.getFlowId();

        JSONObject jsonObject = JSONObject.parseObject(config);
        if (jsonObject == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 从配置中提取组件属性
        String name = jsonObject.getString("name"); // 组件名称
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }

        // 从配置中提取组件数据
        JSONObject data = jsonObject.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 从配置中提取组件属性
        String dataSourceId = extractDataSourceId(data);
        String schemaName = data.getString("schemaName");// 模式名称
        String tableName = extractTableName(data);
        String commitSize = extractCommitSize(data);
        boolean truncateTable = data.getBooleanValue("truncateTable");// 写入之前清空表
        boolean specifyFields = data.getBooleanValue("specifyFields"); // 指定数据库字段
        JSONArray fieldList = data.getJSONArray("fieldList"); // 数据库字段
        boolean ignoreErrors = data.getBooleanValue("ignoreErrors"); // 忽略插入错误
        boolean useBatchUpdate = data.getBooleanValue("useBatchUpdate"); // 是否批量插入
        String partitioningType = data.getString("partitioningType"); // 分区表类型
        String partitioningField = data.getString("partitioningField"); // 分区字段
        String partitioningTimeRadio = data.getString("partitioningTimeRadio"); // 按月或天
        String tableNameField = data.getString("tableNameField"); // 包含表名的字段
        boolean tableNameInTable = data.getBooleanValue("tableNameInTable"); // 存储表名字段
        boolean returningGeneratedKeys = data.getBooleanValue("returningGeneratedKeys"); // 返回自动产生的关键字
        String generatedKeyField = data.getString("generatedKeyField"); // 自动产生的关键字的字段名称
        boolean distributeType = data.getBooleanValue("distributeType"); // 分发或复制

        // 处理字段映射
        FieldMappingResult fieldMappingResult = processFieldMapping(fieldList);

        // 创建数据库元数据
        DatabaseMeta databaseMeta = createDatabaseMeta(dataSourceId, flowId, databaseService, aesUtil, schemaName);

        // 创建表输出元数据
        TableOutputMeta tableOutputMeta = createTableOutputMeta(databaseMeta, schemaName, tableName, commitSize, 
                truncateTable, ignoreErrors, useBatchUpdate, partitioningType, partitioningField, 
                partitioningTimeRadio, tableNameField, tableNameInTable, returningGeneratedKeys, 
                generatedKeyField, specifyFields, fieldMappingResult);

        // 添加数据库到转换元数据
        transMeta.addDatabase(databaseMeta);

        // 创建步骤元数据
        return createStepMeta(registryID, map, id, name, tableOutputMeta, data, distributeType);
    }

    /**
     * 提取组件名称
     * @param data 配置数据
     * @return 组件名称
     */
    private String extractComponentName(JSONObject data) {
        String name = data.getString("name");
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }
        return name;
    }

    /**
     * 提取数据源ID
     * @param data 配置数据
     * @return 数据源ID
     */
    private String extractDataSourceId(JSONObject data) {
        String dataSourceId = data.getString("dataSourceId");
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("数据源ID不能为空！");
        }
        return dataSourceId;
    }

    /**
     * 提取表名称
     * @param data 配置数据
     * @return 表名称
     */
    private String extractTableName(JSONObject data) {
        String tableName = data.getString("tableName");
        if (StringUtils.isBlank(tableName)) {
            throw new BusinessException("表名称不能为空！");
        }
        return tableName;
    }

    /**
     * 提取提交记录数量
     * @param data 配置数据
     * @return 提交记录数量
     */
    private String extractCommitSize(JSONObject data) {
        String commitSize = data.getString("commitSize");
        return StringUtils.isBlank(commitSize) ? "10000" : commitSize;
    }

    /**
     * 处理字段映射
     * @param fieldList 字段列表
     * @return 字段映射结果
     */
    private FieldMappingResult processFieldMapping(JSONArray fieldList) {
        List<String> fieldDatabaseList = new ArrayList<>();
        List<String> fieldStreamList = new ArrayList<>();

        if (fieldList != null) {
            List<FieldMappingVo> fieldMappingVoList = fieldList.toJavaList(FieldMappingVo.class);
            for (FieldMappingVo fieldMappingVo : fieldMappingVoList) {
                fieldDatabaseList.add(fieldMappingVo.getFieldDatabase());
                fieldStreamList.add(fieldMappingVo.getFieldStream());
            }
        }

        return new FieldMappingResult(
                fieldDatabaseList.toArray(new String[0]),
                fieldStreamList.toArray(new String[0])
        );
    }

    /**
     * 创建数据库元数据
     * 
     * @param dataSourceId    数据源ID
     * @param flowId          流程ID
     * @param databaseService 数据库服务
     * @param aesUtil         加密工具
     * @param schemaName      模式名称
     * @return DatabaseMeta 数据库元数据
     */
    private DatabaseMeta createDatabaseMeta(String dataSourceId, Integer flowId,
            DbDatabaseService databaseService, AESUtil aesUtil,
            String schemaName) {
        DatabaseMeta dataMeta = new DatabaseMeta();

        if (StringUtils.isNotBlank(dataSourceId)) {
            DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
            if (database == null) {
                throw new BusinessException("数据源不存在!");
            }
            
            String kettleDbType = ListenDatabaseType.listenDatabaseType(database.getType());
            String connectType = getConnectType(database);
            String decodePassword = aesUtil.decrypt(database.getPassword());

            dataMeta = buildDatabaseMeta(flowId, database, kettleDbType, connectType, decodePassword);
            configureDatabaseProperties(dataMeta, database, kettleDbType, schemaName);
        }

        return dataMeta;
    }

    /**
     * 获取数据库连接类型
     * 
     * @param database 数据库信息
     * @return 连接类型
     */
    private String getConnectType(DbDatabase database) {
        if (StringUtils.isNotBlank(database.getExtConfig())) {
            JSONObject extConfig = JSONObject.parseObject(database.getExtConfig());
            return extConfig.getString("connectType");
        }
        return null;
    }

    /**
     * 构建数据库元数据
     * 
     * @param flowId         流程ID
     * @param database       数据库信息
     * @param kettleDbType   Kettle数据库类型
     * @param connectType    连接类型
     * @param decodePassword 解密后的密码
     * @return DatabaseMeta 数据库元数据
     */
    private DatabaseMeta buildDatabaseMeta(Integer flowId, DbDatabase database,
            String kettleDbType, String connectType,
            String decodePassword) {
        String databaseId = String.format("%s_%s", flowId,
                MD5Util.encode(
                        database.getDbHost() + database.getDbPort() + database.getDbName() + database.getUsername()));

        if (Constants.ORACLE_CONNECT_TYPE.SERVICE.equals(connectType)) {
            String dbName = String.format(
                    "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)))",
                    database.getDbHost(), database.getDbPort(), database.getDbName());
            DatabaseMeta dataMeta = new DatabaseMeta(databaseId, kettleDbType, "Native", "",
                    dbName, "-1", database.getUsername(), decodePassword);
            dataMeta.setServername(database.getDbName());
            return dataMeta;
        } else {
            return new DatabaseMeta(databaseId, kettleDbType, "Native",
                    database.getDbHost(), database.getDbName(), database.getDbPort(),
                    database.getUsername(), decodePassword);
        }
    }

    /**
     * 配置数据库属性
     * 
     * @param dataMeta     数据库元数据
     * @param database     数据库信息
     * @param kettleDbType Kettle数据库类型
     * @param schemaName   数据库schema名称
     */
    private void configureDatabaseProperties(DatabaseMeta dataMeta, DbDatabase database, String kettleDbType,
            String schemaName) {
        // 设置数据库 schema
        if (StringUtils.isNotBlank(schemaName)) {
            dataMeta.setPreferredSchemaName(schemaName);
        } else if (StringUtils.isNotBlank(database.getDbSchema())) {
            dataMeta.setPreferredSchemaName(database.getDbSchema());
        }

        // 配置MySQL相关属性
        if (dataMeta.isMySQLVariant()) {
            configureMySQLProperties(dataMeta, kettleDbType);
        }

        // 配置达梦数据库属性
        if (Constants.DATABASE_TYPE.DM8.equals(database.getType())) {
            configureDMDatabaseProperties(dataMeta, database);
        }

        // 配置扩展属性
        configureExtraProperties(dataMeta, database, kettleDbType);
    }

    /**
     * 配置MySQL数据库属性
     * 
     * @param dataMeta     数据库元数据
     * @param kettleDbType Kettle数据库类型
     */
    private void configureMySQLProperties(DatabaseMeta dataMeta, String kettleDbType) {
        dataMeta.addExtraOption(kettleDbType, "characterEncoding", "UTF-8");
        dataMeta.addExtraOption(kettleDbType, "useServerPrepStmts", "false");
        dataMeta.addExtraOption(kettleDbType, "rewriteBatchedStatements", "true");
        dataMeta.addExtraOption(kettleDbType, "useCompression", "true");
        dataMeta.addExtraOption(kettleDbType, "useSSL", "false");
        dataMeta.addExtraOption(kettleDbType, "serverTimezone", "Asia/Shanghai");
        dataMeta.addExtraOption(kettleDbType, "zeroDateTimeBehavior", "convertToNull");
    }

    /**
     * 配置达梦数据库属性
     * 
     * @param dataMeta 数据库元数据
     * @param database 数据库信息
     */
    private void configureDMDatabaseProperties(DatabaseMeta dataMeta, DbDatabase database) {
        Properties attributes = dataMeta.getAttributes();
        attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_URL,
                String.format("jdbc:dm://%s:%s?", database.getDbHost(), database.getDbPort()));
        attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_DRIVER_CLASS, "dm.jdbc.driver.DmDriver");
        dataMeta.setAttributes(attributes);
    }

    /**
     * 配置扩展属性
     * 
     * @param dataMeta     数据库元数据
     * @param database     数据库信息
     * @param kettleDbType Kettle数据库类型
     */
    private void configureExtraProperties(DatabaseMeta dataMeta, DbDatabase database, String kettleDbType) {
        String properties = database.getProperties();
        if (StringUtils.isNotBlank(properties)) {
            JSONObject proObj = JSONObject.parseObject(properties);
            for (String key : proObj.keySet()) {
                dataMeta.addExtraOption(kettleDbType, key, proObj.getString(key));
            }
        }
    }

    /**
     * 创建表输出元数据
     * @param databaseMeta 数据库元数据
     * @param schemaName 模式名称
     * @param tableName 表名称
     * @param commitSize 提交记录数量
     * @param truncateTable 是否清空表
     * @param ignoreErrors 是否忽略错误
     * @param useBatchUpdate 是否批量更新
     * @param partitioningType 分区类型
     * @param partitioningField 分区字段
     * @param partitioningTimeRadio 分区时间粒度
     * @param tableNameField 表名字段
     * @param tableNameInTable 是否在表中存储表名
     * @param returningGeneratedKeys 是否返回生成的键
     * @param generatedKeyField 生成的键字段
     * @param specifyFields 是否指定字段
     * @param fieldMappingResult 字段映射结果
     * @return 表输出元数据
     */
    private TableOutputMeta createTableOutputMeta(DatabaseMeta databaseMeta, String schemaName, String tableName, 
                                               String commitSize, boolean truncateTable, boolean ignoreErrors, 
                                               boolean useBatchUpdate, String partitioningType, 
                                               String partitioningField, String partitioningTimeRadio, 
                                               String tableNameField, boolean tableNameInTable, 
                                               boolean returningGeneratedKeys, String generatedKeyField, 
                                               boolean specifyFields, FieldMappingResult fieldMappingResult) {
        TableOutputMeta tableOutputMeta = new TableOutputMeta();
        tableOutputMeta.setDefault();
        tableOutputMeta.setDatabaseMeta(databaseMeta);
        tableOutputMeta.setSchemaName(schemaName);
        tableOutputMeta.setTableName(tableName);
        tableOutputMeta.setCommitSize(StringUtils.isBlank(commitSize) ? "10000" : commitSize);
        tableOutputMeta.setSpecifyFields(true);
        tableOutputMeta.setTruncateTable(truncateTable);
        tableOutputMeta.setIgnoreErrors(ignoreErrors);
        tableOutputMeta.setUseBatchUpdate(useBatchUpdate);
        
        // 配置分区
        configurePartitioning(tableOutputMeta, partitioningType, partitioningField, 
                            partitioningTimeRadio, tableNameField, tableNameInTable);
        
        tableOutputMeta.setReturningGeneratedKeys(returningGeneratedKeys);
        tableOutputMeta.setGeneratedKeyField(generatedKeyField);
        tableOutputMeta.setSpecifyFields(specifyFields);
        
        // 配置字段映射
        if (specifyFields) {
            tableOutputMeta.setFieldStream(fieldMappingResult.getFieldStreamArray());
            tableOutputMeta.setFieldDatabase(fieldMappingResult.getFieldDatabaseArray());
        }
        
        return tableOutputMeta;
    }

    /**
     * 配置分区
     * @param tableOutputMeta 表输出元数据
     * @param partitioningType 分区类型
     * @param partitioningField 分区字段
     * @param partitioningTimeRadio 分区时间粒度
     * @param tableNameField 表名字段
     * @param tableNameInTable 是否在表中存储表名
     */
    private void configurePartitioning(TableOutputMeta tableOutputMeta, String partitioningType, 
                                     String partitioningField, String partitioningTimeRadio, 
                                     String tableNameField, boolean tableNameInTable) {
        if (StringUtils.isNotBlank(partitioningType)) {
            // 按时间分区
            if (Constants.PARTITIONING_TYPE.TIME.equals(partitioningType)) {
                tableOutputMeta.setPartitioningEnabled(true);
                tableOutputMeta.setPartitioningField(partitioningField);
                
                // 按月份
                if (Constants.PARTITIONING_TIME.MONTH.equals(partitioningTimeRadio)) {
                    tableOutputMeta.setPartitioningMonthly(true);
                }
                // 按天
                if (Constants.PARTITIONING_TIME.DAY.equals(partitioningTimeRadio)) {
                    tableOutputMeta.setPartitioningDaily(true);
                }
            } else if (Constants.PARTITIONING_TYPE.CUSTOMIZE.equals(partitioningType)) {
                tableOutputMeta.setTableNameInField(true);
                tableOutputMeta.setTableNameField(tableNameField);
                tableOutputMeta.setTableNameInTable(tableNameInTable);
            }
        }
    }

    /**
     * 创建步骤元数据
     * @param registryID 插件注册表
     * @param map 步骤元数据映射
     * @param id 步骤ID
     * @param name 步骤名称
     * @param tableOutputMeta 表输出元数据
     * @param data 配置数据
     * @param distributeType 是否分发
     * @return 步骤元数据
     */
    private StepMeta createStepMeta(PluginRegistry registryID, Map<String, StepMeta> map, 
                                   String id, String name, TableOutputMeta tableOutputMeta, 
                                   JSONObject data, boolean distributeType) {
        String tableOutputPluginId = registryID.getPluginId(StepPluginType.class, tableOutputMeta);

        StepMeta stepMeta = map.get(id);
        if (null == stepMeta) {
            stepMeta = new StepMeta(tableOutputPluginId, name, tableOutputMeta);
        } else {
            stepMeta.setStepID(tableOutputPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(tableOutputMeta);
        }
        
        // 设置复制数
        Integer copiesCache = data.getInteger("copiesCache");
        if (null != copiesCache) {
            stepMeta.setCopies(copiesCache);
        }

        // 设置分发类型
        if (distributeType) {
            stepMeta.setDistributes(true);
        }

        return stepMeta;
    }

    /**
     * 字段映射结果
     */
    private static class FieldMappingResult {
        private final String[] fieldDatabaseArray;
        private final String[] fieldStreamArray;

        public FieldMappingResult(String[] fieldDatabaseArray, String[] fieldStreamArray) {
            this.fieldDatabaseArray = fieldDatabaseArray;
            this.fieldStreamArray = fieldStreamArray;
        }

        public String[] getFieldDatabaseArray() {
            return fieldDatabaseArray;
        }

        public String[] getFieldStreamArray() {
            return fieldStreamArray;
        }
    }
}
