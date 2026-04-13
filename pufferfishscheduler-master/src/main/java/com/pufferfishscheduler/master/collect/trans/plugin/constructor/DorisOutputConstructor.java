package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.plugin.DorisStreamLoaderMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Objects;

/**
 * Doris输出组件构造器
 */
public class DorisOutputConstructor extends AbstractStepMetaConstructor {

    /**
     * 创建Doris输出组件
     *
     * @param config    组件配置
     * @param transMeta 转换元数据
     * @param context   组件上下文
     * @return Doris输出组件
     */
    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取数据库服务和AES工具
        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);

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
            throw new BusinessException("【" + name + "】组件数据不能为空！");
        }

        // 创建Doris输出组件
        DorisStreamLoaderMeta dorisStreamLoaderMeta = new DorisStreamLoaderMeta();
        dorisStreamLoaderMeta.setDefault();

        // 是否清空表
        Boolean truncateTable = data.getBoolean("truncateTable");
        dorisStreamLoaderMeta.setTruncateTable(!Objects.isNull(truncateTable) ? truncateTable : false);

        String dataSourceId = data.getString("dataSourceId"); // 数据源ID
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("【" + name + "】数据源ID不能为空！");
        }
        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("【" + name + "】数据源不存在!");
        }

        // 用户名
        dorisStreamLoaderMeta.setUsername(database.getUsername());
        // 密码
        dorisStreamLoaderMeta.setPassword(aesUtil.decrypt(database.getPassword()));
        // 数据库名称
        dorisStreamLoaderMeta.setDatabase(database.getDbName());
        // ip
        dorisStreamLoaderMeta.setHost(database.getDbHost());
        // 端口
        dorisStreamLoaderMeta.setPort(database.getDbPort());
        // 数据库类型
        dorisStreamLoaderMeta.setDatabaseType(database.getType());
        // loadUrl
        JSONObject extConfig = JSONObject.parseObject(database.getExtConfig());
        dorisStreamLoaderMeta.setFenodes(extConfig.getString("feAddress"));
        // 扩展配置
        dorisStreamLoaderMeta.setExtConfig(database.getExtConfig());

        // 数据表名
        String tableName = data.getString("tableName");
        dorisStreamLoaderMeta.setTable(null == tableName || "".equals(tableName) ? null : tableName);

        // 重试次数
        Integer retries = data.getInteger("retries");
        dorisStreamLoaderMeta.setMaxRetries(null == retries ? 3 : retries);

        // 刷新频率（毫秒）
        Long scanningFrequency = data.getLong("scanningFrequency");
        dorisStreamLoaderMeta.setScanningFrequency(null == scanningFrequency ? 500 : scanningFrequency);

        // 连接超时（秒）
        Long connectTimeout = data.getLong("connectTimeout");
        dorisStreamLoaderMeta.setConnectTimeout(null == connectTimeout ? 3000 : connectTimeout);

        // Stream Load载入数据超时时间（秒）
        Long loadTimeout = data.getLong("timeout");
        dorisStreamLoaderMeta.setLoadTimeout(null == loadTimeout ? 600 : loadTimeout);

        // 最大批次记录数
        Long maxBatchRows = data.getLong("maxBatchRows");
        dorisStreamLoaderMeta.setBufferFlushMaxRows(null == maxBatchRows ? 500000 : maxBatchRows);

        // 最大批次导入量（MB）
        Long maxBytes = data.getLong("maxBytes");
        dorisStreamLoaderMeta.setBufferFlushMaxBytes(null == maxBytes ? 90 * 1024 * 1024 : maxBytes * 1024 * 1024);

        // Stream Load配置
        JSONArray headerProperties = data.getJSONArray("headerProperties");
        if (null != headerProperties && !headerProperties.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append(dorisStreamLoaderMeta.getStreamLoadProp());
            for (Object headerProperty : headerProperties) {
                JSONObject json = JSONObject.parseObject(headerProperty.toString());
                builder.append(json.getString("key")).append(":").append(json.getString("value")).append(";");
            }
            dorisStreamLoaderMeta.setStreamLoadProp(builder.toString());
        }

        // 数据合并方式
        String upsertOrDelete = data.getString("upsertOrDelete");
        if (null != upsertOrDelete && !"".equals(upsertOrDelete)) {
            dorisStreamLoaderMeta.setMergeType(upsertOrDelete);
        }

        // 插入更新
        String partialUpdate = data.getString("partialUpdate");
        if (null != partialUpdate && !"".equals(partialUpdate)) {
            dorisStreamLoaderMeta.setPartialColumns(partialUpdate);
        }

        // 字段
        JSONArray fieldLists = data.getJSONArray("fieldList");
        if (null == fieldLists) {
            fieldLists = new JSONArray();
        }
        int size = fieldLists.size();
        if (size > 0) {
            String[] fieldTable = new String[size];
            String[] fieldStream = new String[size];
            for (int i = 0; i < size; i++) {
                JSONObject object = JSONObject.parseObject(fieldLists.get(i).toString());
                fieldTable[i] = object.getString("fieldDatabase");
                fieldStream[i] = object.getString("fieldStream");
            }
            dorisStreamLoaderMeta.setFieldTable(fieldTable);
            dorisStreamLoaderMeta.setFieldStream(fieldStream);
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, dorisStreamLoaderMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, dorisStreamLoaderMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(dorisStreamLoaderMeta);
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
}
