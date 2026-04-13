package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.writetolog.WriteToLogMeta;

/**
 * 打印日志构造器
 */
@Slf4j
public class WriteToLogConstructor extends AbstractStepMetaConstructor {

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
        WriteToLogMeta writeToLogMeta = new WriteToLogMeta();
        writeToLogMeta.setDefault();

        Boolean limitRows = data.getBoolean("limitRows");
        writeToLogMeta.setLimitRows(null != limitRows && limitRows);
        Integer limitRowsNumber = data.getInteger("limitRowsNumber");
        writeToLogMeta.setLimitRowsNumber(null == limitRowsNumber ? 1000 : limitRowsNumber);

        JSONArray fieldsJson = data.getJSONArray("fieldList");
        if(null == fieldsJson){
            fieldsJson = new JSONArray();
        }

        String[] fieldName = new String[fieldsJson.size()];
        for(int i=0;i< fieldsJson.size();i++){
            JSONObject jo = fieldsJson.getJSONObject(i);
            fieldName[i] = jo.getString("name");
        }

        writeToLogMeta.setFieldName(fieldName);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, writeToLogMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, writeToLogMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(writeToLogMeta);
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
