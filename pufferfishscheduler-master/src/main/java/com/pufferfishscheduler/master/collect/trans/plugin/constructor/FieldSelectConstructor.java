package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;

import java.util.List;

/**
 * 字段选择构造器
 */
public class FieldSelectConstructor extends AbstractStepMetaConstructor {

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

        // 字段选择
        SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
        selectValuesMeta.setDefault();

        JSONArray selectFieldJson = data.getJSONArray("fieldList");
        if (null == selectFieldJson) {
            selectFieldJson = new JSONArray();
        }

        List<SelectValuesMeta.SelectField> selectFields = selectFieldJson.toJavaList(SelectValuesMeta.SelectField.class);
        SelectValuesMeta.SelectField[] fields = new SelectValuesMeta.SelectField[selectFields.size()];
        for (int i = 0; i < selectFields.size(); i++) {
            fields[i] = selectFields.get(i);
        }

        selectValuesMeta.setSelectFields(fields);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, selectValuesMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, selectValuesMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(selectValuesMeta);
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
