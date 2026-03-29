package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.condition.SqlConditionBuilder;
import com.pufferfishscheduler.plugin.common.model.DatabaseConnectionInfo;
import com.pufferfishscheduler.plugin.common.util.AESUtil;
import com.pufferfishscheduler.plugin.common.util.JdbcUrlUtil;
import com.pufferfishscheduler.plugin.common.util.JdbcUtil;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 值映射
 */
public class ValueMapping implements RuleProcessor {
    private static final int MAPPING_MANUAL = 0;
    private static final int MAPPING_DB_VIEW = 1;
    private static final int MAPPING_CUSTOM_SQL = 2;

    private JSONObject data;
    private Integer mappingType; // 映射方式

    private final Map<String, Object> cacheDatabase = new HashMap<>(); // 存储数据库配置

    private final Map<String, Object> cacheManual = new HashMap<>(); // 存储手工配置

    /**
     * 初始化
     *
     * @param metadata
     * @throws KettleStepException
     */
    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (!metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.MAPPING_TYPE)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.MAPPING_TYPE);
        }
        mappingType = (Integer) data.getOrDefault(Constants.MAPPING_TYPE, null);
        if (mappingType == null) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.MAPPING_TYPE);
        }
    }

    @Override
    public Object convert(Object value) throws KettleException, UnsupportedEncodingException {
        String stringValue = value == null ? null : String.valueOf(value);
        // 判断使用的是哪种映射方式
        switch (mappingType) {
            case MAPPING_MANUAL: // 手工配置
                return dealManualConfig(data, stringValue);
            case MAPPING_DB_VIEW: // 数据库可视化配置
                return dealDataViewConfig(data, stringValue);
            case MAPPING_CUSTOM_SQL: // 自定义sql
                return dealCustomSQL(data, stringValue);
            default:
                throw new KettleException("未知的映射方式：" + mappingType);
        }
    }

    /**
     * 自定义sql
     *
     * @param data
     * @param value
     */
    private Object dealCustomSQL(JSONObject data, String value) throws KettleException {
        DatabaseConnectionInfo connectionInfo = buildConnectionInfo(data);
        String sql = data.getString("sql");

        // 校验sql是否符合规范
        SqlConditionBuilder.validateCustomSql(sql);

        return dealDataBaseConnect(value, connectionInfo, sql);
    }


    /**
     * 数据库字典表
     *
     * @param data
     * @param value
     */
    private Object dealDataViewConfig(JSONObject data, String value) throws KettleException {
        DatabaseConnectionInfo connectionInfo = buildConnectionInfo(data);
        String dbType = connectionInfo.getType();

        // 可视化配置 → 直接用工具类构建
        String beforeFieldName = data.getString("beforefieldName");
        String afterFieldName = data.getString("afterfieldName");
        String tableName = data.getString("tableName");

        // 构建带schema的表名 → 复用工具类
        tableName = SqlConditionBuilder.buildFullTableName(dbType, connectionInfo.getDbSchema(), connectionInfo.getDbName(), tableName);

        // 构建WHERE条件 → 工具类
        String whereClause = "";
        JSONObject condition = data.getJSONObject("condition");
        if (condition != null) {
            whereClause = SqlConditionBuilder.buildWhereClause(condition, dbType);
        }

        // 拼接查询SQL（只查2列）
        String sql = String.format("SELECT %s, %s FROM %s %s",
                beforeFieldName, afterFieldName, tableName, whereClause);
        return dealDataBaseConnect(value, connectionInfo, sql);
    }

    /**
     * 处理数据库的jdbc连接
     *
     * @param value          原始值
     * @param connectionInfo 数据库连接信息
     * @param sql            查询语句
     * @return
     * @throws KettleException
     */
    private Object dealDataBaseConnect(String value, DatabaseConnectionInfo connectionInfo, String sql) throws KettleException {
        if (cacheDatabase.isEmpty()) {
            String dbType = connectionInfo.getType();
            try {
                String driver = JdbcUrlUtil.getDriver(dbType);
                String url = JdbcUrlUtil.getUrl(connectionInfo.getType(), connectionInfo.getDbHost(), connectionInfo.getDbPort(), connectionInfo.getDbName(), connectionInfo.getProperties());
                try (Connection conn = JdbcUtil.getConnection(driver, url, connectionInfo);
                     Statement stat = conn.createStatement();
                     ResultSet rs = stat.executeQuery(sql)) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    SqlConditionBuilder.mapPreviewResult(cacheDatabase, rs, rsmd);
                }
            } catch (SQLException | KettleException e) {
                throw new KettleException("[值映射]:" + dbType + Constants.DATABASE_OPERATE_EXCEPTIPN + e.getMessage());
            }
        }
        return getMappedOrDefault(cacheDatabase, value);
    }

    /**
     * 手工配置
     *
     * @param data
     * @param value
     */
    private Object dealManualConfig(JSONObject data, String value) throws KettleStepException {
        JSONArray fieldList = (JSONArray) data.getOrDefault(Constants.FIELD_LIST, null);
        if (fieldList == null) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.FIELD_LIST);
        }
        if (value == null) {
            return null;
        }

        if (cacheManual.isEmpty()) {
            for (int i = 0; i < fieldList.size(); i++) {
                JSONObject object = fieldList.getJSONObject(i);
                String code = object.getString("code");
                Object name = object.get("valueName");

                putValuesIntoCache(cacheManual, code, name);
            }
        }

        return getMappedOrDefault(cacheManual, value);
    }

    /**
     * 处理值中存在的 ,
     *
     * @param cache
     * @param keys
     * @param value
     */
    private void putValuesIntoCache(Map<String, Object> cache, String keys, Object value) {
        if (keys == null || keys.trim().isEmpty()) {
            return;
        }
        if (keys.contains(",")) {
            for (String key : keys.split(",")) {
                cache.put(key.trim(), value);
            }
        } else {
            cache.put(keys.trim(), value);
        }
    }

    /**
     * 从缓存中获取映射值或默认值
     *
     * @param cache 缓存
     * @param value 原始值
     * @return 映射值或默认值
     */
    private Object getMappedOrDefault(Map<String, Object> cache, String value) {
        if (value == null) {
            return null;
        }
        Object codeValue = cache.get(value);
        return Objects.isNull(codeValue) ? value : codeValue;
    }

    /**
     * 构建数据库连接信息
     *
     * @param data
     * @return
     */
    private DatabaseConnectionInfo buildConnectionInfo(JSONObject data) {
        DatabaseConnectionInfo connectionInfo = new DatabaseConnectionInfo();
        connectionInfo.setType(data.getString("dbType"));
        connectionInfo.setDbHost(data.getString("host"));
        connectionInfo.setDbPort(String.valueOf(data.get("port")));
        connectionInfo.setDbName(data.getString("dbName"));
        connectionInfo.setDbSchema(data.getString("schema"));
        connectionInfo.setUsername(data.getString("username"));
        connectionInfo.setPassword(AESUtil.decrypt(data.getString("password")));
        connectionInfo.setProperties(data.getString("properties"));
        return connectionInfo;
    }
}
