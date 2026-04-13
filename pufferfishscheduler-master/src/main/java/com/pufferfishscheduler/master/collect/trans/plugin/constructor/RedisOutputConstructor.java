package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.plugin.RedisOutputStepMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * 创建Redis输出步骤
 */
public class RedisOutputConstructor  extends AbstractStepMetaConstructor {

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
            throw new BusinessException("组件数据不能为空！");
        }

        // 构建RedisOutput对象
        RedisOutputStepMeta redisOutputStepMeta = new RedisOutputStepMeta();
        redisOutputStepMeta.setDefault();

        String dataSourceId = data.getString("dataSourceId"); // 数据源ID
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("数据源ID不能为空！");
        }

        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        redisOutputStepMeta.setHost(database.getDbHost());
        redisOutputStepMeta.setPort(database.getDbPort());
        redisOutputStepMeta.setPassword(aesUtil.decrypt(database.getPassword()));
        redisOutputStepMeta.setDbName(data.getString("dbName"));
        redisOutputStepMeta.setKey(data.getString("key"));
        redisOutputStepMeta.setValue(data.getString("value"));


        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, redisOutputStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, redisOutputStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(redisOutputStepMeta);
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
