package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.NormaliserFieldVo;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.normaliser.NormaliserMeta;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 列转行组件构造器
 */
public class NormaliserConstructor extends AbstractStepMetaConstructor {

    /**
     * 创建组件实例
     *
     * @param config    流程配置json
     * @param transMeta 转换元数据
     * @param context   上下文参数
     * @return
     */
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
        NormaliserMeta normaliserMeta = new NormaliserMeta();
        normaliserMeta.setDefault();

        String keyField = data.getString("keyField");         // 关键字段
        normaliserMeta.setTypeField(keyField);

        String valueField = data.getString("valueField");       // value字段名称

        List<NormaliserFieldVo> normaliserFieldList = new ArrayList<>();
        JSONArray normaliserFields = data.getJSONArray("normaliserFields");
        if (CollectionUtils.isNotEmpty(normaliserFields)) {
            normaliserFieldList = normaliserFields.toJavaList(NormaliserFieldVo.class);
        }

        normaliserMeta.allocate(normaliserFieldList.size());

        // 循环normaliserFields集合，为集合中的元素NormaliserField对象的norm属性赋值
        NormaliserMeta.NormaliserField[] normaliserField = new NormaliserMeta.NormaliserField[normaliserFieldList.size()];
        for (int i = 0; i < normaliserFieldList.size(); i++) {
            NormaliserMeta.NormaliserField field = new NormaliserMeta.NormaliserField();
            BeanUtils.copyProperties(normaliserFieldList.get(i), field);
            field.setNorm(valueField);
            normaliserField[i] = field;
        }
        normaliserMeta.setNormaliserFields(normaliserField);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, normaliserMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, normaliserMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(normaliserMeta);
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
