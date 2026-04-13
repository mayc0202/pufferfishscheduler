package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.ProcessBranchSetupVo;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseMeta;
import org.pentaho.di.trans.steps.switchcase.SwitchCaseTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 流程分支
 */
public class ProcessBranchConstructor extends AbstractStepMetaConstructor {

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

        // 默认分支
        String defaultTargetStep = data.getString("defaultTargetStep");

        Map<String, StepMeta> map = context.getStepMetaMap();

        // 构造器
        SwitchCaseMeta switchCaseMeta = new SwitchCaseMeta();
        // 条件字段
        String conditionField = data.getString("conditionFiled");
        switchCaseMeta.setFieldname(conditionField);

        // 流程分支设置
        JSONArray processBranchSetup = data.getJSONArray("ProcessBranchSetup");
        List<SwitchCaseTarget> caseTargets = new ArrayList<>();
        List<ProcessBranchSetupVo> resultList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(processBranchSetup)) {
            resultList = processBranchSetup.toJavaList(ProcessBranchSetupVo.class);
        }
        for (ProcessBranchSetupVo vo : resultList) {
            SwitchCaseTarget switchCaseTarget = new SwitchCaseTarget();
            if (StringUtils.isNotBlank(vo.getStepId())) {
                StepMeta stepMeta = map.get(vo.getStepId());
                if (null == stepMeta) {
                    stepMeta = new StepMeta();
                }
                switchCaseTarget.caseTargetStep = stepMeta;
                switchCaseTarget.caseValue = vo.getCaseValue();
                map.put(vo.getStepId(), stepMeta);
                caseTargets.add(switchCaseTarget);
            }
        }
        switchCaseMeta.setCaseTargets(caseTargets);
        if (StringUtils.isNotBlank(defaultTargetStep)) {
            StepMeta stepMeta = map.get(defaultTargetStep);
            if (null == stepMeta) {
                stepMeta = new StepMeta();
            }
            switchCaseMeta.setDefaultTargetStep(stepMeta);
            map.put(defaultTargetStep, stepMeta);
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, switchCaseMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, switchCaseMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(switchCaseMeta);
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
