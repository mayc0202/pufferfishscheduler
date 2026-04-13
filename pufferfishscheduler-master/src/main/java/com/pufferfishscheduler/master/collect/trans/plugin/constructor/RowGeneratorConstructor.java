package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.domain.RowGeneratorMateData;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 生成测试数据构造器
 */
@Slf4j
public class RowGeneratorConstructor extends AbstractStepMetaConstructor {

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
        RowGeneratorMeta rowGeneratorMeta = new RowGeneratorMeta();
        rowGeneratorMeta.setDefault();

        int type = data.getIntValue("recordGenerationType");//生成记录方式
        if (Objects.equals(type, Constants.RECORD_GENERATOR_TYPE.BATCH)) {
            rowGeneratorMeta.setNeverEnding(false);//批量生成记录
            rowGeneratorMeta.setRowLimit((String) data.getOrDefault("rowLimit", "10"));//生成记录条数
        }
        if (Objects.equals(type, Constants.RECORD_GENERATOR_TYPE.CONTINUOUS)) {
            rowGeneratorMeta.setNeverEnding(true);//持续生成记录
            rowGeneratorMeta.setIntervalInMs((String) data.getOrDefault("timeInterval", "100"));//生成记录时间间隔
            rowGeneratorMeta.setRowTimeField(data.getString("timeFieldName"));//生成记录时间字段名
            rowGeneratorMeta.setLastTimeField(data.getString("lastTimeFieldName"));//上次生成记录时间字段名
            rowGeneratorMeta.setRowLimit((String) data.getOrDefault("rowLimit", "10"));
        }
        List<RowGeneratorMateData> voList = new ArrayList<>();
        JSONArray fieldList = data.getJSONArray("fieldList");
        if (CollectionUtils.isNotEmpty(fieldList)) {
            voList = fieldList.toJavaList(RowGeneratorMateData.class);
        }
        String[] fieldName = new String[voList.size()];
        String[] fieldType = new String[voList.size()];
        String[] fieldFormat = new String[voList.size()];
        int[] fieldLength = new int[voList.size()];
        int[] fieldPrecision = new int[voList.size()];
        String[] decimal = new String[voList.size()];
        String[] value = new String[voList.size()];
        String[] group = new String[voList.size()];
        rowGeneratorMeta.allocate(voList.size());
        for (int i = 0; i < voList.size(); i++) {
            RowGeneratorMateData vo = voList.get(i);
            fieldName[i] = vo.getFieldName();
            fieldType[i] = vo.getTypeText();
            fieldFormat[i] = vo.getFieldFormat();
            fieldLength[i] = vo.getFieldLength();
            fieldPrecision[i] = vo.getFieldPrecision();
            decimal[i] = vo.getDecimal();
            value[i] = vo.getValue();
            group[i] = vo.getGroup();
        }

        //设置字段
        rowGeneratorMeta.setFieldName(fieldName);
        rowGeneratorMeta.setFieldType(fieldType);
        rowGeneratorMeta.setFieldFormat(fieldFormat);
        rowGeneratorMeta.setFieldLength(fieldLength);
        rowGeneratorMeta.setFieldPrecision(fieldPrecision);
        rowGeneratorMeta.setDecimal(decimal);
        rowGeneratorMeta.setValue(value);
        rowGeneratorMeta.setGroup(group);


        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, rowGeneratorMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, rowGeneratorMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(rowGeneratorMeta);
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
