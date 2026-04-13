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
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.sql.ExecSQLMeta;

import java.util.Properties;

/**
 * 执行SQL脚本 构造器
 */
public class SQLScriptConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {

        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取数据库服务和AES工具
        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);

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

        // 创建组件实例
        ExecSQLMeta execSQLMeta = new ExecSQLMeta();
        execSQLMeta.setDefault();

        // 数据源ID
        String dataSourceId = data.getString("dataSourceId");
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("数据源ID不能为空！");
        }

        // SQL脚本
        String sql = data.getString("sql");
        if (StringUtils.isBlank(sql)) {
            throw new BusinessException("SQL脚本不能为空！");
        }
        execSQLMeta.setSql(sql);

        DatabaseMeta dataMeta = createDatabaseMeta(dataSourceId, flowId, databaseService, aesUtil, data);
        transMeta.addDatabase(dataMeta);
        execSQLMeta.setDatabaseMeta(dataMeta);

        boolean executeEachRow = data.getBooleanValue("executeEachRow");
        execSQLMeta.setExecutedEachInputRow(executeEachRow);//执行每一行
        if (executeEachRow) {
            execSQLMeta.setParams(data.getBooleanValue("bindVariable"));//绑定变量
        }
        execSQLMeta.setVariableReplacementActive(data.getBooleanValue("variableReplace"));//变量替换
        JSONArray fieldList = data.getJSONArray("filedList");
        if (CollectionUtils.isNotEmpty(fieldList)) {
            String[] arguments = fieldList.toArray(new String[fieldList.size()]);
            execSQLMeta.setArguments(arguments);//设置参数字段
        } else {
            execSQLMeta.setArguments(new String[0]);
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, execSQLMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, execSQLMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(execSQLMeta);
        }
        Integer copiesCache = data.getInteger("copiesCache");
        if (null != copiesCache) {
            stepMeta.setCopies(copiesCache);
        }
        //判断是否为复制流程
        boolean distributeType = data.getBooleanValue("distributeType");
        if (distributeType) {
            stepMeta.setDistributes(false);
        }

        return stepMeta;
    }

    /**
     * 创建数据库元数据
     *
     * @param dataSourceId    数据源ID
     * @param flowId          流程ID
     * @param databaseService 数据库服务
     * @param aesUtil         加密工具
     * @param data            配置数据
     * @return DatabaseMeta 数据库元数据
     */
    private DatabaseMeta createDatabaseMeta(String dataSourceId, Integer flowId,
                                            DbDatabaseService databaseService, AESUtil aesUtil,
                                            JSONObject data) {
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

}
