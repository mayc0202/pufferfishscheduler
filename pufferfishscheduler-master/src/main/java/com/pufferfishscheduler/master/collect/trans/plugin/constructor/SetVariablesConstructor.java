package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.form.collect.SetVariableForm;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.setvariable.SetVariableMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * 设置变量
 */
public class SetVariablesConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        JSONObject jsonObject = JSONObject.parseObject(config);
        if (jsonObject == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 从配置中提取组件属性
        String name = jsonObject.getString("name");
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }

        // 从配置中提取组件数据
        JSONObject data = jsonObject.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 构建meta对象
        SetVariableMeta setVariableMeta = new SetVariableMeta();
        setVariableMeta.setDefault();

        JSONArray fieldList = data.getJSONArray("fieldList");
        List<SetVariableForm> formList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fieldList)){
            formList = fieldList.toJavaList(SetVariableForm.class);
        }

        setVariableMeta.allocate(formList.size());
        String[] fieldName = new String[formList.size()];
        String[] variableName = new String[formList.size()];
        int[] variableType = new int[formList.size()];

        for (int i = 0; i < formList.size(); i++) {
            SetVariableForm form = formList.get(i);
            fieldName[i] = form.getFieldName();
            variableName[i] = form.getVariableName();
            variableType[i] = form.getVariableType();
        }

        setVariableMeta.setFieldName(fieldName);
        setVariableMeta.setVariableName(variableName);
        setVariableMeta.setVariableType(variableType);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, setVariableMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, setVariableMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(setVariableMeta);
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
