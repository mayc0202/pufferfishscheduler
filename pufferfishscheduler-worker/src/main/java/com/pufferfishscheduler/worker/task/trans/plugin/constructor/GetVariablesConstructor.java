package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.getvariable.GetVariableMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取变量构造器
 */
public class GetVariablesConstructor extends AbstractStepMetaConstructor {

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

        // 构建meta对象
        GetVariableMeta getVariableMeta = new GetVariableMeta();
        getVariableMeta.setDefault();

        JSONArray fieldList = data.getJSONArray("fieldList");
        List<GetVariableMeta.FieldDefinition> list = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fieldList)){
            list = fieldList.toJavaList(GetVariableMeta.FieldDefinition.class);
        }
        GetVariableMeta.FieldDefinition[] fields = new GetVariableMeta.FieldDefinition[list.size()];
        for (int i = 0; i < list.size(); i++) {
            fields[i] = list.get(i);
        }

        getVariableMeta.setFieldDefinitions(fields);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, getVariableMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, getVariableMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(getVariableMeta);
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
