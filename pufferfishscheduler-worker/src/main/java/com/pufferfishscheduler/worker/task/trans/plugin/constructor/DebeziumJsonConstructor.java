package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import com.pufferfishscheduler.plugin.DebeziumJsonStepMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * Debezium报文解析 构造器
 */
public class DebeziumJsonConstructor extends AbstractStepMetaConstructor {

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

        // 创建组件实例
        DebeziumJsonStepMeta debeziumStepMeta = new DebeziumJsonStepMeta();
        debeziumStepMeta.setDefault();

        // 数据源字段
        String sourceField = data.getString("sourceField");
        debeziumStepMeta.setSourceField(sourceField);

        // 输出字段
        JSONArray fieldLists = data.getJSONArray("fieldList");
        if (null == fieldLists) {
            fieldLists = new JSONArray();
        }
        if (!fieldLists.isEmpty()) {
            String[] outputFieldList = new String[fieldLists.size()];
            for (int i = 0; i < fieldLists.size(); i++) {
                outputFieldList[i] = JSONObject.toJSONString(fieldLists.get(i));
            }
            debeziumStepMeta.setOutputFieldConfig(outputFieldList);
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, debeziumStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, debeziumStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(debeziumStepMeta);
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
