package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.steps.javafilter.JavaFilterMeta;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Java条件判断
 */
public class JavaConditionConstructor extends AbstractStepMetaConstructor {

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

        // java条件构造器
        JavaFilterMeta filterMeta = new JavaFilterMeta();
        filterMeta.setDefault();

        String condition = (String) data.getOrDefault("condition", "true");//条件
        String trueStepName = data.getString("trueStepName");//正确的步骤名
        String errorStepName = data.getString("errorStepName");//错误的步骤名
        if (context.isValidate()) {
            validateBlank(condition, "【" + name + "】", "条件(Java表达式)");
            validateBlank(trueStepName, "【" + name + "】", "接受匹配行的步骤");
            validateBlank(errorStepName, "【" + name + "】", "接受不匹配行的步骤");
        }
        filterMeta.setCondition(condition);

        Map<String, StepMeta> map = context.getStepMetaMap();
        // 正确的step
        StepMeta trueStep = map.get(trueStepName);
        if (Objects.isNull(trueStep)) {
            trueStep = new StepMeta();
            map.put(trueStepName, trueStep);
        }

        // 错误的step
        StepMeta errorStep = map.get(errorStepName);
        if (Objects.isNull(errorStep)) {
            errorStep = new StepMeta();
            map.put(errorStepName, errorStep);
        }

        List<StreamInterface> targetStreams = filterMeta.getStepIOMeta().getTargetStreams();
        targetStreams.get(0).setStepMeta(trueStep);
        targetStreams.get(1).setStepMeta(errorStep);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, filterMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, filterMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(filterMeta);
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
