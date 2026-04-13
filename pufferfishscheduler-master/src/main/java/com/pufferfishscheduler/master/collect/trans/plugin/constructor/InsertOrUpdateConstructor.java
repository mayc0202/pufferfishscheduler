package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.ListenDatabaseType;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import com.pufferfishscheduler.plugin.util.DbDatabaseVo;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;

/**
* 插入或更新构造器
 */
public class InsertOrUpdateConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

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

        String dataSourceId = data.getString("dataSourceId");

        String tableName = data.getString("tableName");

        String commitSize = data.getString("commitSize");

        Boolean updateBypassed = data.getBoolean("updateBypassed");

        if (context.isValidate()) {
            validateBlank(dataSourceId, "【" + name + "】", "数据源");
        }
        String dbType = null;
        String kettleDbType = null;

//        DatabaseMeta dataMeta = new DatabaseMeta();
//        if (StringUtils.isNotBlank(dataSourceId)) {
//            DbDatabaseVo dbVo = dbDao.getDbDataBaseByDbId(Integer.valueOf(dataSourceId));
//            dbType = dbVo.getType();
//            kettleDbType = ListenDatabaseType.listenDatabaseType(dbVo.getType());
//
//            String connectType = null;
//            if (StringUtils.isNotBlank(dbVo.getExtConfig())) {
//                JSONObject extConfig = JSONObject.parseObject(dbVo.getExtConfig());
//                connectType = extConfig.getString("connectType");
//            }
//
//            if (Constants.ORACLE_CONNECT_TYPE.SERVICE.equals(connectType)) {
//                String dbName = String.format("(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)))", dbVo.getDbHost(), dbVo.getDbPort(), dbVo.getDbName());
//                dataMeta = new DatabaseMeta(String.format("%s_%s", flowId, MD5.encode(dbVo.getDbHost() + dbVo.getDbPort() + dbVo.getDbName() + dbVo.getUsername())), kettleDbType, "Native", "", dbName, "-1", dbVo.getUsername(), TripleDESUtil.decrypt3DES(dbVo.getPassword()));
//                dataMeta.setServername(dbVo.getDbName());
//            } else {
//                dataMeta = new DatabaseMeta(String.format("%s_%s", flowId,
//                        MD5.encode(dbVo.getDbHost() + dbVo.getDbPort() + dbVo.getDbName() + dbVo.getUsername())),
//                        kettleDbType,
//                        "Native",
//                        dbVo.getDbHost(),
//                        dbVo.getDbName(),
//                        dbVo.getDbPort(),
//                        dbVo.getUsername(),
//                        TripleDESUtil.decrypt3DES(dbVo.getPassword()));//解密后的密码
//            }
//
//            if (StringUtils.isNotBlank(dbVo.getDbSchema())) {
//                dataMeta.setPreferredSchemaName(dbVo.getDbSchema());
//            }
//
//            if (dataMeta.isMySQLVariant()) {
//                dataMeta.addExtraOption(kettleDbType, "characterEncoding", "UTF-8");
//                dataMeta.addExtraOption(kettleDbType, "useServerPrepStmts", "false");
//                dataMeta.addExtraOption(kettleDbType, "rewriteBatchedStatements", "true");
//                dataMeta.addExtraOption(kettleDbType, "useCompression", "true");
//                dataMeta.addExtraOption(kettleDbType, "useSSL", "false");
//                dataMeta.addExtraOption(kettleDbType, "serverTimezone", "Asia/Shanghai");
//                dataMeta.addExtraOption(kettleDbType, "zeroDateTimeBehavior", "convertToNull");
//            }
//
//            Properties attributes = dataMeta.getAttributes();
//
//            if (Constants.DbType.cache.equals(dbVo.getType())) {
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_URL, String.format("jdbc:IRIS://%s:%s/%s?", dbVo.getDbHost(), dbVo.getDbPort(), dbVo.getDbName()));
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_DRIVER_CLASS, "com.intersystems.jdbc.IRISDriver");
//            } else if (Constants.DbType.kyuubi.equals(dbVo.getType())) {
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_URL, String.format("jdbc:kyuubi://%s:%s?", dbVo.getDbHost(), dbVo.getDbPort()));
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_DRIVER_CLASS, "org.apache.kyuubi.jdbc.KyuubiHiveDriver");
//            } else if (Constants.DbType.dm.equals(dbVo.getType())) {
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_URL, String.format("jdbc:dm://%s:%s?", dbVo.getDbHost(), dbVo.getDbPort()));
//                attributes.put(Constants.CACHE_CONFIG.ATRRIBUTE_CUSTOM_DRIVER_CLASS, "dm.jdbc.driver.DmDriver");
//            }
//
//            dataMeta.setAttributes(attributes);
//
//            if(Constants.DbType.kyuubi.equals(dbVo.getType())){
//                dataMeta.addExtraOption(kettleDbType, "kyuubi.engine.type", "JDBC");
//            }
//
//            //扩展属性解析拼接
//            String properties = dbVo.getProperties();
//            if (StringUtils.isNotBlank(properties)) {
//                JSONObject proObj = JSONObject.parseObject(properties);
//                for (String key : proObj.keySet()) {
//                    dataMeta.addExtraOption(kettleDbType, key, proObj.getString(key));
//                }
//            }
//
//            if(org.apache.commons.lang3.StringUtils.isNotBlank(dbVo.getDbSchema())){
//                DB_SCHEMA = dbVo.getDbSchema();
//            }
//        }
//        DBCache.getInstance().clear(dataMeta.getName());
//        transMeta.addDatabase(dataMeta);
//        Boolean distributeType = data.getBoolean("distributeType");
//
//
//        /**
//         * 插入/更新
//         */
//        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
//
//        //设置数据源参数
//        insertUpdateMeta.setDatabaseMeta(dataMeta);
//        //设置表名称
//        insertUpdateMeta.setTableName(tableName);
//        //设置提交记录数量
//        insertUpdateMeta.setCommitSize(commitSize);
//
//        if(StringUtils.isNotBlank(DB_SCHEMA)){
//            insertUpdateMeta.setSchemaName(DB_SCHEMA);
//        }
//
//        if (null != updateBypassed) {
//            //设置是否只执行插入操作
//            insertUpdateMeta.setUpdateBypassed(updateBypassed);
//        }
//
//
//        //设置用来查询的关键字
//        JSONArray selectField = data.getJSONArray("selectField");
//
//        if (null == selectField) {
//            selectField = new JSONArray();
//        }
//
//        String[] keyStream = new String[selectField.size()];
//
//        String[] keyLookup = new String[selectField.size()];
//
//        String[] keyCondition = new String[selectField.size()];
//
//        String[] keyStream2 = new String[selectField.size()];
//
//        for (int i = 0; i < selectField.size(); i++) {
//            JSONObject jo = selectField.getJSONObject(i);
//            keyLookup[i] = jo.getString("keyLookup");
//            keyCondition[i] = jo.getString("keyCondition");
//            keyStream[i] = jo.getString("keyStream");
//            keyStream2[i] = jo.getString("keyStream2");
//        }
//
//        insertUpdateMeta.setKeyStream(keyStream);
//        insertUpdateMeta.setKeyLookup(keyLookup);
//        insertUpdateMeta.setKeyCondition(keyCondition);
//        insertUpdateMeta.setKeyStream2(keyStream2);
//
//        //设置更新字段
//        JSONArray updateField = data.getJSONArray("updateField");
//
//        if (null == updateField) {
//            updateField = new JSONArray();
//        }
//
//        String[] updateLookup = new String[updateField.size()];
//
//        String[] updateStream = new String[updateField.size()];
//
//        Boolean[] update = new Boolean[updateField.size()];
//
//        String[] functionList = new String[updateField.size()];
//
//        for (int i = 0; i < updateField.size(); i++) {
//            JSONObject jo = updateField.getJSONObject(i);
//            updateLookup[i] = jo.getString("updateLookup");
//            updateStream[i] = jo.getString("updateStream");
//            update[i] = jo.getBoolean("update");
//            if (null != jo.getBoolean("spatialConversion") && jo.getBoolean("spatialConversion")) {
//                functionList[i] = SpatialConversionUtil.getSpatialConversionFunction(dbType, jo.getInteger("coordinateSystem"));
//            } else {
//                functionList[i] = "";
//            }
//        }
//
//        insertUpdateMeta.setUpdateLookup(updateLookup);
//        insertUpdateMeta.setUpdateStream(updateStream);
//        insertUpdateMeta.setUpdate(update);
//        insertUpdateMeta.setFunctions(functionList);
//
//        // 从插件注册表中获取Excel输入插件的插件ID
//        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, insertUpdateMeta);
//        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
//        if (null == stepMeta) {
//            stepMeta = new StepMeta(eiPluginId, name, insertUpdateMeta);
//        } else {
//            stepMeta.setStepID(eiPluginId);
//            stepMeta.setName(name);
//            stepMeta.setStepMetaInterface(insertUpdateMeta);
//        }
//
//        Integer copiesCache = data.getInteger("copiesCache");
//        if (null != copiesCache) {
//            stepMeta.setCopies(copiesCache);
//        }
//
//        //判断是否为复制流程
//        boolean distributeType = data.getBooleanValue("distributeType");
//        if (distributeType) {
//            stepMeta.setDistributes(false);
//        }
//
//        return stepMeta;

        return null;
    }
}
