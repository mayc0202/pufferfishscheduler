package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.splitfieldtorows.SplitFieldToRowsMeta;

/**
 * 字段拆分为多行构造器
 */
public class SplitFieldToRowsConstructor extends AbstractStepMetaConstructor {

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

        // 创建分隔字段为多行元数据
        SplitFieldToRowsMeta splitFieldToRowsMeta = new SplitFieldToRowsMeta();
        splitFieldToRowsMeta.setDefault();

        String splitField = data.getString("splitField");      // 需要拆分的字段名称
        String delimiter = data.getString("delimiter");         // 分隔符
        String newFieldname = data.getString("newFieldname");  // 输出字段名称
        if (context.isValidate()){
            validateBlank(splitField, "【"+splitField+"】", "需要拆分的字段名称");
            validateBlank(delimiter, "【"+delimiter+"】", "分隔符");
            validateBlank(newFieldname, "【"+newFieldname+"】", "输出字段名称");
        }

        splitFieldToRowsMeta.setSplitField(splitField);
        splitFieldToRowsMeta.setDelimiter(delimiter);
        splitFieldToRowsMeta.setNewFieldname(newFieldname);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, splitFieldToRowsMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, splitFieldToRowsMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(splitFieldToRowsMeta);
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
