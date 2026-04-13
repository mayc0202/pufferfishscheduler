package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.FieldInfoVo;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassDef;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Java代码插件构造器
 */
@Slf4j
public class JavaCodeConstructor extends AbstractStepMetaConstructor {

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

        UserDefinedJavaClassMeta javaCodeMeta = new UserDefinedJavaClassMeta();

        JSONArray fieldList = data.getJSONArray("fieldList");//字段

        List<UserDefinedJavaClassMeta.FieldInfo> fieldInfoList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(fieldList)) {

            List<FieldInfoVo> fieldInfo = fieldList.toJavaList(FieldInfoVo.class);
            for (FieldInfoVo vo : fieldInfo) {

                Integer length = vo.getLength();
                Integer precision = vo.getPrecision();
                if(Objects.isNull(vo.getLength())){
                    length = -1;
                }
                if(Objects.isNull(precision)){
                    precision = -1;
                }
                UserDefinedJavaClassMeta.FieldInfo field = new UserDefinedJavaClassMeta.FieldInfo(
                        vo.getName(), vo.getType(), length, precision);
                fieldInfoList.add(field);
            }
            javaCodeMeta.setFieldInfo(fieldInfoList);
        }

        String javaCode = data.getString("javaCode");//Java代码

        UserDefinedJavaClassDef userDefinedJavaClassDef = new UserDefinedJavaClassDef(
                UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, "Processor", javaCode
        );
        List<UserDefinedJavaClassDef> definitions = new ArrayList<>();
        definitions.add(userDefinedJavaClassDef);
        javaCodeMeta.replaceDefinitions(definitions);

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, javaCodeMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, javaCodeMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(javaCodeMeta);
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
