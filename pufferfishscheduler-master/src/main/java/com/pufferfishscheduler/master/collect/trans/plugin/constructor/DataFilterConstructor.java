package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.plugin.DataFilterStepMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
* 数据过滤组件构造器
 */
public class DataFilterConstructor extends AbstractStepMetaConstructor {

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
        DataFilterStepMeta filterStepMeta = new DataFilterStepMeta();
        filterStepMeta.setDefault();

        // 转换方式
        String filterType = data.getString("filterType");
        filterStepMeta.setFilterType(filterType);
        if (filterType != null && Integer.parseInt(filterType) == 0) {
            JSONObject condition = data.getJSONObject("condition");
            filterStepMeta.setCondition(condition.toString());
            filterStepMeta.setJavaCode(null);
        } else {
            String javaCode = data.getString("javaCode");
            filterStepMeta.setJavaCode(javaCode);
            filterStepMeta.setCondition(null);
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, filterStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, filterStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(filterStepMeta);
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
