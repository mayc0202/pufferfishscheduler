package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.DenormalizedFieldVo;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.denormaliser.DenormaliserMeta;
import org.pentaho.di.trans.steps.denormaliser.DenormaliserTargetField;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 行转列组件构造器
 */
public class DenormalizedConstructor extends AbstractStepMetaConstructor {

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

        //
        DenormaliserMeta denormaliserMeta = new DenormaliserMeta();
        denormaliserMeta.setDefault();

        String keyField = data.getString("keyField");           // 关键字段
        denormaliserMeta.setKeyField(keyField);

        List<String> groupFieldList = new ArrayList<>();               // 分组字段
        String groupField = data.getString("groupField");
        groupFieldList.add(groupField);

        List<DenormalizedFieldVo> denormaliserTargetFieldVOList = new ArrayList<>();   // 获取目标字段
        JSONArray denormaliserTargetField = data.getJSONArray("denormaliserTargetField");
        String targetColumnName = "";
        if (CollectionUtils.isNotEmpty(denormaliserTargetField)) {
            denormaliserTargetFieldVOList = denormaliserTargetField.toJavaList(DenormalizedFieldVo.class);
            targetColumnName = denormaliserTargetField.getJSONObject(0).getString("fieldName"); // 获取到取值字段名称
        }

        // 将获取到的分组字段设置到denormaliserMeta对象的groupField属性上
        String[] groupFields = new String[groupFieldList.size()];
        for (int i = 0; i < groupFieldList.size(); i++) {
            groupFields[i] = groupFieldList.get(i);
        }
        denormaliserMeta.setGroupField(groupFields);

        int targetType = 2;    // 默认数据类型
        int targetColumnIndex = -1;     // targetColumnName列的索引值
        // 获取上一步的执行结果
        Result previousResult = transMeta.getPreviousResult();
        if (Objects.nonNull(previousResult) && !previousResult.getRows().isEmpty()) {
            List<RowMetaAndData> rows = previousResult.getRows();
            RowMetaInterface rowMeta = rows.get(0).getRowMeta();
            for (int i = 0; i < rowMeta.size(); i++) {
                if (targetColumnName.equals(rowMeta.getValueMeta(i).getName())) {
                    targetColumnIndex = i;  // 找到了targetColumnName列的索引值
                    break;
                }
            }

            if (targetColumnIndex >= 0) {
                // 使用正确的列索引获取数据类型
                ValueMetaInterface valueMeta = rowMeta.getValueMeta(targetColumnIndex);
                targetType = valueMeta.getType();
            }
        }

        // 将获取到的目标字段设置到denormaliserMeta对象的denormaliserTargetField属性上
        DenormaliserTargetField[] denormalizedTargetFields = new DenormaliserTargetField[denormaliserTargetFieldVOList.size()];
        for (int i = 0; i < denormaliserTargetFieldVOList.size(); i++) {
            DenormalizedFieldVo normaliserTargetFieldVO = denormaliserTargetFieldVOList.get(i);
            DenormaliserTargetField denormalizedTargetField1 = new DenormaliserTargetField();
            BeanUtils.copyProperties(normaliserTargetFieldVO, denormalizedTargetField1);
            denormalizedTargetField1.setTargetType(targetType);
            denormalizedTargetFields[i] = denormalizedTargetField1;
        }
        denormaliserMeta.setDenormaliserTargetField(denormalizedTargetFields);


        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, denormaliserMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, denormaliserMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(denormaliserMeta);
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
