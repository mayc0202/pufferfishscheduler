package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.ListenDatabaseType;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.database.editor.AbstractQueryEditor;
import com.pufferfishscheduler.master.database.editor.DatabaseEditorFactory;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;

import lombok.extern.slf4j.Slf4j;

/**
 * 表输入组件构造器
 */
@Slf4j
public class TableInputConstructor extends AbstractStepMetaConstructor {

    /**
     * 创建表输入组件
     * 
     * @param config    流程配置json
     * @param transMeta 转换元数据
     * @param context   上下文参数
     * @return StepMeta 步骤元数据
     */
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

        String dataSourceId = data.getString("dataSourceId"); // 数据源ID
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("数据源ID不能为空！");
        }
        String rowLimit = data.getString("rowLimit"); // 行限制，如果为空则不限制
        String sql = data.getString("sql"); // SQL查询语句
        if (StringUtils.isBlank(sql)) {
            throw new BusinessException("SQL查询语句不能为空！");
        }
        Boolean replaceVariable = data.getBoolean("replaceVariable"); // 是否替换变量
        Boolean implementEveryOne = data.getBoolean("implementEveryOne"); // 是否每个实例都执行
        String stepInsertVariable = data.getString("stepInsertVariable"); // 步骤插入变量
        boolean increment = data.getBoolean("increment") != null ? data.getBoolean("increment") : false; // 是否增量读取

        TableInputMeta tableInput = new TableInputMeta();
        tableInput.setDefault();
        tableInput.setVariableReplacementActive(null != replaceVariable ? replaceVariable : false);

        DatabaseMeta dataMeta = createDatabaseMeta(dataSourceId, flowId, databaseService, aesUtil, data, sql, increment,
                tableInput);
        transMeta.addDatabase(dataMeta);

        tableInput.setDatabaseMeta(dataMeta);
        // 设置行限制：如果用户指定了rowLimit则使用用户指定的值，否则设置为0表示不限制
        if (StringUtils.isNotBlank(rowLimit)) {
            tableInput.setRowLimit(rowLimit);
        } else {
            // 显式设置为0，表示不限制读取行数
            tableInput.setRowLimit("0");
        }

        configureStepVariables(tableInput, stepInsertVariable, implementEveryOne, map);
        return createStepMeta(registryID, map, id, name, tableInput, data);
    }

    /**
     * 创建数据库元数据
     * 
     * @param dataSourceId    数据源ID
     * @param flowId          流程ID
     * @param databaseService 数据库服务
     * @param aesUtil         加密工具
     * @param data            配置数据
     * @param sql             SQL语句
     * @param increment       是否增量
     * @param tableInput      表输入元数据
     * @return DatabaseMeta 数据库元数据
     */
    private DatabaseMeta createDatabaseMeta(String dataSourceId, Integer flowId,
            DbDatabaseService databaseService, AESUtil aesUtil,
            JSONObject data, String sql, boolean increment,
            TableInputMeta tableInput) {
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
            configureDatabaseProperties(dataMeta, database, kettleDbType);

            // 处理增量查询
            if (increment) {
                sql = handleIncrementQuery(database, data, sql, tableInput);
            }

            tableInput.setSQL(sql);
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
     */
    private void configureDatabaseProperties(DatabaseMeta dataMeta, DbDatabase database, String kettleDbType) {
        // 设置数据库 schema
        if (StringUtils.isNotBlank(database.getDbSchema())) {
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
        // 设置MySQL的fetchSize为Integer.MIN_VALUE，表示不限制读取行数
        // 这是MySQL JDBC驱动的特殊要求，必须配合useCursorFetch=true使用
        dataMeta.addExtraOption(kettleDbType, "defaultFetchSize", String.valueOf(Integer.MIN_VALUE));
        dataMeta.addExtraOption(kettleDbType, "useCursorFetch", "true");
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
     * 处理增量查询
     * 
     * @param database   数据库信息
     * @param data       配置数据
     * @param sql        SQL语句
     * @param tableInput 表输入元数据
     * @return 处理后的SQL语句
     */
    private String handleIncrementQuery(DbDatabase database, JSONObject data, String sql, TableInputMeta tableInput) {
        AbstractQueryEditor editor = DatabaseEditorFactory.getDatabaseEditor(database.getType());
        String incrementType = data.getString("incrementType");
        String incrementField = data.getString("incrementField");

        if (StringUtils.isBlank(incrementType) || StringUtils.isBlank(incrementField)) {
            throw new BusinessException("增量查询配置不完整!");
        }

        sql = editor.spellIncrementSql(sql, incrementType, incrementField);
        tableInput.setVariableReplacementActive(true);
        return sql;
    }

    /**
     * 配置步骤变量
     * 
     * @param tableInput         表输入元数据
     * @param stepInsertVariable 插入变量的步骤
     * @param implementEveryOne  是否为每行执行
     * @param map                步骤元数据映射
     */
    private void configureStepVariables(TableInputMeta tableInput, String stepInsertVariable,
            Boolean implementEveryOne, Map<String, StepMeta> map) {
        if (StringUtils.isNotBlank(stepInsertVariable)) {
            StepMeta stepMeta = map.get(stepInsertVariable);
            if (null == stepMeta) {
                stepMeta = new StepMeta();
            }
            tableInput.setLookupFromStep(stepMeta);
            tableInput.setExecuteEachInputRow(implementEveryOne);
            map.put(stepInsertVariable, stepMeta);
        }
    }

    /**
     * 创建步骤元数据
     * 
     * @param registryID 插件注册表
     * @param map        步骤元数据映射
     * @param id         步骤ID
     * @param name       步骤名称
     * @param tableInput 表输入元数据
     * @param data       配置数据
     * @return StepMeta 步骤元数据
     */
    private StepMeta createStepMeta(PluginRegistry registryID, Map<String, StepMeta> map,
            String id, String name, TableInputMeta tableInput, JSONObject data) {
        String eiPluginId = registryID.getPluginId(StepPluginType.class, tableInput);
        StepMeta stepMeta = map.get(id);
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, tableInput);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(tableInput);
        }

        stepMeta.setDraw(true);
        stepMeta.setLocation(100, 100);

        // 设置复制数
        Integer copiesCache = data.getInteger("copiesCache");
        if (null != copiesCache) {
            stepMeta.setCopies(copiesCache);
        }

        // 判断是否为复制流程
        boolean distributeType = data.getBooleanValue("distributeType");
        if (distributeType) {
            stepMeta.setDistributes(false);
        }

        return stepMeta;
    }
}
