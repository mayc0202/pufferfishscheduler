package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.FieldSplitterVo;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.fieldsplitter.FieldSplitterMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * 字段拆分为多列构造器
 */
public class FieldSplitterConstructor extends AbstractStepMetaConstructor {

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

        // 创建字段拆分为多列元数据
        FieldSplitterMeta fieldSplitterMeta = new FieldSplitterMeta();
        fieldSplitterMeta.setDefault();

        // 需要拆分的字段名称
        String splitField = data.getString("splitField");
        fieldSplitterMeta.setSplitField(splitField);

        // 分隔符
        String delimiter = data.getString("delimiter");
        fieldSplitterMeta.setDelimiter(delimiter);

        // 输出字段配置
        List<FieldSplitterVo> fieldSplitterVos = new ArrayList<>();
        JSONArray fieldList = data.getJSONArray("fieldList");// 输出字段配置
        if (CollectionUtils.isNotEmpty(fieldList)) {
            fieldSplitterVos = fieldList.toJavaList(FieldSplitterVo.class);
        }

        fieldSplitterMeta.allocate(fieldSplitterVos.size());

        String[] fieldNames = new String[fieldSplitterVos.size()];       // 输出字段名称
        int[] fieldTrimTypes = new int[fieldSplitterVos.size()];         // 输出字段是否需要去除空格
        int[] fieldTypes = new int[fieldSplitterVos.size()];             // 输出字段类型

        for (int i = 0; i < fieldSplitterVos.size(); i++) {
            FieldSplitterVo fieldSplitterVO = fieldSplitterVos.get(i);
            fieldNames[i] = fieldSplitterVO.getFieldName();
            fieldTrimTypes[i] = fieldSplitterVO.getFieldTrimType();
            fieldTypes[i] = fieldSplitterVO.getFieldType();
        }

        // 设置字段
        fieldSplitterMeta.setFieldName(fieldNames);
        fieldSplitterMeta.setFieldType(fieldTypes);
        fieldSplitterMeta.setFieldTrimType(fieldTrimTypes);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, fieldSplitterMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, fieldSplitterMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(fieldSplitterMeta);
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
