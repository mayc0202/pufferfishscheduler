package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import com.pufferfishscheduler.plugin.DataCleanStepMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据清洗插件构造器
 */
@Slf4j
public class DataCleanConstructor extends AbstractStepMetaConstructor {

    /**
     * 构造数据清洗插件元数据
     *
     * @param config    插件配置
     * @param transMeta 转换元数据
     * @param context   插件上下文
     * @return 插件元数据
     */
    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        PluginRegistry registryID = context.getRegistryID();
        Map<String, StepMeta> map = context.getStepMetaMap();
        String id = context.getId();

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

        // 注意：不能直接依赖插件 jar 的 StepMeta 类（不同 ClassLoader 会导致 registry.getPluginId 返回 null）
        // 正确做法：始终通过 Kettle 插件注册表的 ClassLoader 加载并实例化 StepMeta
        DataCleanStepMeta cleanStepMeta = new DataCleanStepMeta();
        cleanStepMeta.setDefault();
        cleanStepMeta.setName(name);
        // api地址
        cleanStepMeta.setAddress(context.getUrl());

        JSONArray fieldList = data.getJSONArray("fieldList");

        if (null == fieldList) {
            fieldList = new JSONArray();
        }
        // 校验清洗字段
        checkFieldList(context.isValidate(), fieldList);

        if (!fieldList.isEmpty()) {
            List<String> fieldNameList = new ArrayList<>();
            List<String> paramsList = new ArrayList<>();
            List<String> renameList = new ArrayList<>();
            List<String> renameTypeList = new ArrayList<>();
            List<String> ruleIdList = new ArrayList<>();
            // 处理数据格式
            for (int i = 0; i < fieldList.size(); i++) {
                JSONObject object = JSONObject.parseObject(fieldList.get(i).toString());
                String fieldName = object.getString("name");
                if ("".equals(fieldName) || fieldName == null) {
                    throw new BusinessException(name + "组件：请选择清洗字段名称！");
                }
                JSONArray ruleList = object.getJSONArray("ruleList");
                if (ruleList != null && !ruleList.isEmpty()) {
                    for (int j = 0; j < ruleList.size(); j++) {
                        JSONObject json = JSONObject.parseObject(ruleList.get(j).toString());
                        JSONObject metadata = new JSONObject();
                        metadata.put("data", json);
                        String ruleId = json.getString("ruleId");
                        String rename = json.getString("rename");
                        String renameType = json.getString("renameType");

                        fieldNameList.add(fieldName);
                        paramsList.add(metadata.toString());
                        ruleIdList.add(ruleId);
                        renameList.add(rename);
                        renameTypeList.add(renameType);
                    }
                }
            }
            // 赋值
            cleanStepMeta.setFieldName(fieldNameList.toArray(new String[0]));
            cleanStepMeta.setParams(paramsList.toArray(new String[0]));
            cleanStepMeta.setRename(renameList.toArray(new String[0]));
            cleanStepMeta.setRenameType(renameTypeList.toArray(new String[0]));
            cleanStepMeta.setRuleId(ruleIdList.toArray(new String[0]));
        }

        // 兼容历史流程里可能使用的旧插件ID
        String eiPluginId = resolveAvailablePluginId(registryID, cleanStepMeta);
        StepMeta stepMeta = map.get(id);
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, cleanStepMeta);
        } else {
            //赋值
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(cleanStepMeta);
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

        // 返回需要的数据
        return stepMeta;
    }

    /**
     * 解析可用的插件ID
     *
     * @param registry 插件注册表
     * @param cleanStepMeta 清洗插件元数据
     * @return 可用的插件ID
     */
    private String resolveAvailablePluginId(PluginRegistry registry, DataCleanStepMeta cleanStepMeta) {
        // 优先使用 registry 通过 StepMeta 解析真实注册ID，兼容不同插件ID命名方式
        String pluginId = registry.getPluginId(StepPluginType.class, cleanStepMeta);
        if (StringUtils.isNotBlank(pluginId)) {
            return pluginId;
        }

        PluginInterface plugin = registry.findPluginWithId(StepPluginType.class, Constants.StepMetaType.DATA_CLEAN);
        if (plugin != null) {
            return Constants.StepMetaType.DATA_CLEAN;
        }

        // 容错：保存配置时不阻断，回退到约定插件ID，避免因运行节点未及时加载插件导致保存失败
        List<PluginInterface> stepPlugins = registry.getPlugins(StepPluginType.class);
        List<String> pluginIds = new ArrayList<>();
        if (stepPlugins != null) {
            for (PluginInterface pi : stepPlugins) {
                if (pi == null || pi.getIds() == null || pi.getIds().length == 0) {
                    continue;
                }
                String firstId = pi.getIds()[0];
                if (StringUtils.isNotBlank(firstId)) {
                    pluginIds.add(firstId);
                }
            }
        }
        log.warn("未找到数据清洗插件ID[{}]，回退使用默认ID。当前已加载Step插件数量：{}，样例ID：{}",
                Constants.StepMetaType.DATA_CLEAN, pluginIds.size(), pluginIds.stream().limit(10).toList());
        return Constants.StepMetaType.DATA_CLEAN;
    }

    /**
     * 校验清洗字段
     *
     * @param validate  是否校验
     * @param fieldList 清洗字段列表
     */
    private void checkFieldList(boolean validate, JSONArray fieldList) {
        if (validate && fieldList.isEmpty()) {
            throw new BusinessException("清洗字段列表不能为空！");
        }
        fieldList.forEach(f -> {
            JSONObject object = JSONObject.parseObject(f.toString());
            if (object.getJSONArray("ruleList").isEmpty()) {
                throw new BusinessException("已选字段的清洗规则不能为空！");
            }
        });
    }

}
