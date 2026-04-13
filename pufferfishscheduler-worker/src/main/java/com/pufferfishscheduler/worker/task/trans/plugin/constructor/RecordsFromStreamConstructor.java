package com.pufferfishscheduler.worker.task.trans.plugin.constructor;


import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.recordsfromstream.RecordsFromStreamMeta;

/**
 * 从流中获取记录
 */
public class RecordsFromStreamConstructor extends AbstractStepMetaConstructor {

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

        // 构造组件实例
        RecordsFromStreamMeta recordsFromStreamMeta = new RecordsFromStreamMeta();
        recordsFromStreamMeta.setDefault();

        JSONArray fieldList = data.getJSONArray("fieldList");
        if(null == fieldList || fieldList.isEmpty()){
            fieldList = new JSONArray();
        }

        String[] fieldNames = new String[fieldList.size()];
        int[] types = new int[fieldList.size()];
        int[] lengths = new int[fieldList.size()];
        int[] precisions = new int[fieldList.size()];

        //遍历
        for (int i = 0; i < fieldList.size(); i++) {
            JSONObject jo = fieldList.getJSONObject(i);
            fieldNames[i] = jo.getString("name");
            types[i] = jo.getInteger("type");
            lengths[i] = null == jo.getInteger("length") ? -1 : jo.getInteger("length");
            precisions[i] = null == jo.getInteger("precision") ? -1 : jo.getInteger("precision");
        }

        recordsFromStreamMeta.setFieldname(fieldNames);
        recordsFromStreamMeta.setType(types);
        recordsFromStreamMeta.setLength(lengths);
        recordsFromStreamMeta.setPrecision(precisions);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, recordsFromStreamMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, recordsFromStreamMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(recordsFromStreamMeta);
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
