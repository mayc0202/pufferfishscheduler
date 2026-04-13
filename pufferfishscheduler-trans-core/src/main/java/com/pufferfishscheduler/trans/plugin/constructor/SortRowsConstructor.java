package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;

/**
 * 排序构造器
 */
public class SortRowsConstructor extends AbstractStepMetaConstructor {

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

        /**
         * 排序记录
         */
        SortRowsMeta sortRowsMeta = new SortRowsMeta();
        sortRowsMeta.setDefault();

        String sortSize = data.getString("sortSize");
        sortRowsMeta.setSortSize(sortSize);

        JSONArray sortRowsJson = data.getJSONArray("fieldList");
        if(null == sortRowsJson){
            sortRowsJson = new JSONArray();
        }

        String[] fieldName = new String[sortRowsJson.size()];
        boolean[] ascending = new boolean[sortRowsJson.size()];
        boolean[] caseSensitive = new boolean[sortRowsJson.size()];
        boolean[] collatorEnabled = new boolean[sortRowsJson.size()];
        int[] collatorStrength = new int[sortRowsJson.size()];
        boolean[] preSortedField = new boolean[sortRowsJson.size()];

        for(int i=0;i<sortRowsJson.size();i++){
            JSONObject jo = sortRowsJson.getJSONObject(i);
            fieldName[i] = jo.getString("name");
            ascending[i] = "Y".equals(jo.getString("ascending"));
            caseSensitive[i] = "Y".equals(jo.getString("caseSensitive"));
            collatorEnabled[i] = false;
            collatorStrength[i] = 0;
            preSortedField[i] = false;
        }

        sortRowsMeta.setFieldName(fieldName);
        sortRowsMeta.setAscending(ascending);
        sortRowsMeta.setCaseSensitive(caseSensitive);
        sortRowsMeta.setCollatorEnabled(collatorEnabled);
        sortRowsMeta.setCollatorStrength(collatorStrength);
        sortRowsMeta.setPreSortedField(preSortedField);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, sortRowsMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, sortRowsMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(sortRowsMeta);
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
