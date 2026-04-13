package com.pufferfishscheduler.master.collect.trans.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.domain.domain.RuleMetaData;
import com.pufferfishscheduler.domain.form.collect.PreviewForm;
import com.pufferfishscheduler.domain.vo.collect.PreviewVo;
import com.pufferfishscheduler.domain.vo.collect.RuleDetailVo;
import com.pufferfishscheduler.master.collect.rule.service.RuleService;
import com.pufferfishscheduler.master.collect.trans.engine.DataFlowRepository;
import com.pufferfishscheduler.master.collect.trans.engine.DataTransEngine;
import com.pufferfishscheduler.master.collect.trans.engine.TransWrapper;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;
import com.pufferfishscheduler.master.collect.trans.engine.listener.KettleStepListener;
import com.pufferfishscheduler.master.collect.trans.engine.listener.KettleStepRowListener;
import com.pufferfishscheduler.master.collect.trans.engine.listener.KettleTransListener;
import com.pufferfishscheduler.master.collect.trans.engine.logchannel.LogChannel;
import com.pufferfishscheduler.master.collect.trans.engine.logchannel.LogChannelManager;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.collect.trans.plugin.StepMetaConstructorFactory;
import com.pufferfishscheduler.master.collect.trans.service.StepService;
import dm.jdbc.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.debug.BreakPointListener;
import org.pentaho.di.trans.debug.StepDebugMeta;
import org.pentaho.di.trans.debug.TransDebugMeta;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * kettle step service
 */
@Slf4j
@Component
public class StepServiceImpl implements StepService {

    @Value("${plugin.apiUrl}")
    private String apiUrl;

    @Value("${plugin.getRuleInformation}")
    private String getRuleInformation;

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private RuleService ruleService;

    @Autowired
    DataTransEngine dataTransEngine;

    /**
     * 更新数据清洗规则配置
     *
     * @param ruleConfig 数据清洗规则配置
     * @return 更新后的数据清洗规则配置
     */
    @Override
    public String updateDataCleanRuleConfig(String ruleConfig) {
        JSONObject config = JSONObject.parseObject(ruleConfig);
        JSONArray steps = config.getJSONArray("cells");

        JSONArray updatedSteps = processSteps(steps);

        JSONObject resultConfig = new JSONObject(config);
        resultConfig.put("steps", updatedSteps);
        return resultConfig.toString();
    }

    /**
     * 处理所有步骤
     */
    private JSONArray processSteps(JSONArray steps) {
        JSONArray resultSteps = new JSONArray();

        for (Object step : steps) {
            JSONObject stepObj = (JSONObject) step;
            if (isDataCleanStep(stepObj)) {
                resultSteps.add(processDataCleanStep(stepObj));
            } else {
                resultSteps.add(stepObj);
            }
        }

        return resultSteps;
    }

    /**
     * 判断是否为数据清洗步骤
     */
    private boolean isDataCleanStep(JSONObject stepObj) {
        JSONObject data = stepObj.getJSONObject("data");
        String code = data.getString("code");
        return StringUtil.isNotEmpty(code) && Constants.StepMetaType.DATA_CLEAN.equals(code);
    }

    /**
     * 处理数据清洗步骤
     */
    private JSONObject processDataCleanStep(JSONObject stepObj) {
        JSONObject resultStep = new JSONObject(stepObj);
        JSONObject data = stepObj.getJSONObject("data");
        JSONObject outData = new JSONObject(data);

        JSONObject dataObj = data.getJSONObject("data");
        JSONArray fieldList = dataObj.getJSONArray("fieldList");

        if (fieldList != null) {
            JSONArray updatedFieldList = processFieldList(fieldList);
            dataObj.put("fieldList", updatedFieldList);
        }

        outData.put("data", dataObj);
        resultStep.put("data", outData);

        return resultStep;
    }

    /**
     * 处理字段列表
     */
    private JSONArray processFieldList(JSONArray fieldList) {
        JSONArray resultFieldList = new JSONArray();

        for (Object field : fieldList) {
            JSONObject fieldObj = (JSONObject) field;
            JSONArray ruleList = fieldObj.getJSONArray("ruleList");

            if (ruleList != null) {
                JSONArray updatedRuleList = processRuleList(ruleList);
                fieldObj.put("ruleList", updatedRuleList);
            }

            resultFieldList.add(fieldObj);
        }

        return resultFieldList;
    }

    /**
     * 处理规则列表
     */
    private JSONArray processRuleList(JSONArray ruleList) {
        JSONArray resultRuleList = new JSONArray();

        for (Object rule : ruleList) {
            JSONObject ruleObj = (JSONObject) rule;
            Integer groupId = ruleObj.getInteger("groupId");

            if (groupId != null && !groupId.equals(Constants.RULE_MANAGER.GENERIC_TYPE)) {
                RuleMetaData updatedRule = updateRule(ruleObj);
                resultRuleList.add(updatedRule);
            } else {
                resultRuleList.add(ruleObj);
            }
        }

        return resultRuleList;
    }


    /**
     * 更新转换流规则
     *
     * @param ruleVo 转换流规则
     * @return 转换流规则
     */
    public RuleMetaData updateRule(JSONObject ruleVo) {
        RuleMetaData ruleMetaData = JSONObject.parseObject(ruleVo.toString(), RuleMetaData.class);
        String ruleId = ruleVo.getString("ruleId");

        RuleDetailVo rule = ruleService.detail(ruleId);

        if (rule == null) {
            fillRuleFromVo(ruleMetaData, ruleVo);
        } else {
            fillRuleFromDatabase(ruleMetaData, rule);
            enrichRuleWithReleasedConfig(ruleMetaData, rule);
        }

        return ruleMetaData;
    }

    /**
     * 从请求参数填充规则（规则不存在时使用）
     */
    private void fillRuleFromVo(RuleMetaData ruleJson, JSONObject ruleVo) {
        ruleJson.setRuleId(ruleVo.getString("ruleId"));
        ruleJson.setRuleName(ruleVo.getString("ruleName"));
        ruleJson.setRuleCode(ruleVo.getString("ruleCode"));
        ruleJson.setGroupId(ruleVo.getInteger("groupId"));
        ruleJson.setRuleDescription(ruleVo.getString("ruleDescription"));
        ruleJson.setRuleProcessorId(ruleVo.getInteger("ruleProcessorId"));
        wrapperRuleJson(ruleVo, ruleJson);
    }

    /**
     * 从数据库填充规则
     */
    private void fillRuleFromDatabase(RuleMetaData ruleMetaData, RuleDetailVo rule) {
        ruleMetaData.setRuleId(rule.getId());
        ruleMetaData.setRuleName(rule.getRuleName());
        ruleMetaData.setRuleCode(rule.getRuleCode());
        ruleMetaData.setGroupId(rule.getGroupId());
        ruleMetaData.setRuleDescription(rule.getRuleDescription());
        ruleMetaData.setRuleProcessorId(rule.getRuleProcessorId());
    }

    /**
     * 使用已发布的配置信息丰富规则
     */
    private void enrichRuleWithReleasedConfig(RuleMetaData ruleMetaData, RuleDetailVo rule) {
        if (rule.getConfig() == null) {
            return;
        }

        JSONObject releasedConfig = JSONObject.parseObject(rule.getConfig());
        Object data = releasedConfig.get("data");

        if (data != null) {
            JSONObject dataJson = JSONObject.parseObject(data.toString());
            wrapperRuleJson(dataJson, ruleMetaData);
        }
    }

    /**
     * 包装ruleJson（重构版）
     */
    private void wrapperRuleJson(JSONObject dataJson, RuleMetaData ruleMetaData) {
        // fieldList
        if (dataJson.containsKey("fieldList")) {
            ruleMetaData.setFieldList(dataJson.getJSONArray("fieldList"));
        }

        // mappingType
        if (dataJson.containsKey("mappingType")) {
            ruleMetaData.setMappingType(dataJson.getString("mappingType"));
        }

        // dataSourceId
        if (dataJson.containsKey("dataSourceId")) {
            Integer value = dataJson.getInteger("dataSourceId");
            if (Objects.nonNull(value)) {
                ruleMetaData.setDataSourceId(value);
            }
        }

        // tableName
        if (dataJson.containsKey("tableName")) {
            ruleMetaData.setTableName(dataJson.getString("tableName"));
        }

        // beforefieldName
        if (dataJson.containsKey("beforefieldName")) {
            ruleMetaData.setBeforefieldName(dataJson.getString("beforefieldName"));
        }

        // afterfieldName
        if (dataJson.containsKey("afterfieldName")) {
            ruleMetaData.setAfterfieldName(dataJson.getString("afterfieldName"));
        }

        // condition
        if (dataJson.containsKey("condition")) {
            ruleMetaData.setOuterQueryParams(dataJson.getJSONObject("condition"));
        }

        // sqlCode
        if (dataJson.containsKey("sqlCode")) {
            ruleMetaData.setSqlCode(dataJson.getString("sqlCode"));
        }

        // javaCode
        if (dataJson.containsKey("javaCode")) {
            ruleMetaData.setJavaCode(dataJson.getString("javaCode"));
        }
    }

    /**
     * 构建转换元数据
     *
     * @param flowId   流程ID
     * @param config   配置信息
     * @param validate 是否验证
     * @return 转换元数据
     */
    @Override
    public TransMeta buildTransMeta(Integer flowId, String config, boolean validate) {
        // 参数验证
        if (flowId == null) {
            throw new BusinessException("流程ID不能为空");
        }
        if (StringUtils.isBlank(config)) {
            throw new BusinessException("配置信息不能为空");
        }

        // 保证 Kettle 已加载插件后再用 PluginRegistry 解析步骤 ID（避免保存到 kettle_flow_repository 的 XML 出现空 <type>）
        dataTransEngine.init();

        // 1. 解析整个配置
        JSONObject jsonObject = JSONObject.parseObject(config);
        if (Objects.isNull(jsonObject)) {
            throw new BusinessException("转换流程配置格式错误!");
        }

        // 2. 获取cells数组（前端使用的是cells，不是steps）
        JSONArray cells = jsonObject.getJSONArray("cells");
        if (CollectionUtils.isEmpty(cells)) {
            throw new BusinessException("转换流程配置中不存在步骤或连线!");
        }

        // 3. 分离步骤节点和连线
        JSONArray stepCells = new JSONArray();
        JSONArray edgeCells = new JSONArray();

        for (int i = 0; i < cells.size(); i++) {
            JSONObject cell = cells.getJSONObject(i);
            String shape = cell.getString("shape");
            if ("edge".equals(shape)) {
                edgeCells.add(cell);
            } else {
                stepCells.add(cell);
            }
        }

        if (CollectionUtils.isEmpty(stepCells)) {
            throw new BusinessException("转换流程配置中不存在步骤!");
        }

        // 4. 解析步骤节点，建立步骤名称映射
        Map<String, StepMeta> stepMetaMap = new HashMap<>();
        Map<String, String> stepNames = new HashMap<>();
        Map<String, String> stepTypes = new HashMap<>();

        for (int i = 0; i < stepCells.size(); i++) {
            JSONObject stepCell = stepCells.getJSONObject(i);
            String id = stepCell.getString("id");
            if (StringUtils.isBlank(id)) {
                throw new BusinessException("步骤ID不能为空");
            }

            // 获取步骤数据 - 前端数据直接放在data字段，不需要再取data.data
            JSONObject data = stepCell.getJSONObject("data");
            if (data == null) {
                throw new BusinessException("步骤数据不能为空");
            }

            String name = data.getString("name");
            if (StringUtils.isBlank(name)) {
                throw new BusinessException("步骤名称不能为空");
            }

            String type = data.getString("code");
            if (StringUtils.isBlank(type)) {
                throw new BusinessException("步骤类型不能为空");
            }

            stepNames.put(id, name);
            stepTypes.put(id, type);

            log.debug("解析步骤 - id: {}, name: {}, type: {}", id, name, type);
        }

        // 5. 构建转换流程元数据
        TransMeta transMeta = new TransMeta();
        PluginRegistry registryID = PluginRegistry.getInstance();

        // 存储错误处理配置
        Map<String, Map<String, String>> errorConfigMap = new HashMap<>();

        // 6. 创建步骤
        for (int i = 0; i < stepCells.size(); i++) {
            JSONObject stepCell = stepCells.getJSONObject(i);
            String id = stepCell.getString("id");
            JSONObject data = stepCell.getJSONObject("data");

            // 获取步骤类型和名称
            String type = stepTypes.get(id);
            String name = stepNames.get(id);

            // 根据type生成对应的stepMeta
            AbstractStepMetaConstructor stepMetaConstructor = StepMetaConstructorFactory.getConstructor(type);

            StepContext context = new StepContext();
            context.setRegistryID(registryID);
            context.setStepMetaMap(stepMetaMap);
            context.setId(id);
            context.setValidate(validate);
            context.setFlowId(flowId);
            context.setStepNames(stepNames);

            if (type.equals(Constants.StepMetaType.DATA_CLEAN)) {
                context.setUrl(apiUrl + getRuleInformation);
            }

            // 将data转换为JSON字符串传递给构造函数
            String dataStr = data.toJSONString();
            StepMeta stepMeta = stepMetaConstructor.create(dataStr, transMeta, context);
            stepMeta.setDraw(true);
            stepMeta.setName(name); // 确保设置步骤名称

            // 设置步骤位置
            JSONObject position = stepCell.getJSONObject("position");
            if (position != null) {
                Integer x = position.getInteger("x");
                Integer y = position.getInteger("y");
                if (x != null && y != null) {
                    stepMeta.setLocation(x, y);
                }
            }

            // 判断组件是否支持错误处理
            boolean supportError = data.getBooleanValue("supportError");
            if (supportError) {
                stepMeta.supportsErrorHandling();
                Map<String, String> resultMap = getErrorHandlerConfig(data);
                // 表示启用了错误处理
                if (!resultMap.isEmpty()) {
                    errorConfigMap.put(id, resultMap);
                }
            }

            // 将各个步骤存入map，方便后面连线
            stepMetaMap.put(id, stepMeta);
            transMeta.addStep(stepMeta);

            log.debug("创建步骤 - id: {}, name: {}, type: {}", id, name, type);
        }

        // 7. 解析连线（edge）
        if (!CollectionUtils.isEmpty(edgeCells)) {
            for (int i = 0; i < edgeCells.size(); i++) {
                JSONObject edgeCell = edgeCells.getJSONObject(i);

                // 获取source和target信息
                JSONObject source = edgeCell.getJSONObject("source");
                JSONObject target = edgeCell.getJSONObject("target");

                if (source == null || target == null) {
                    log.warn("连线配置缺少source或target");
                    continue;
                }

                String sourceId = source.getString("cell");
                String targetId = target.getString("cell");

                if (StringUtils.isBlank(sourceId) || StringUtils.isBlank(targetId)) {
                    log.warn("连线配置中source或target的cell为空");
                    continue;
                }

                // 从map中获取线两端的步骤
                StepMeta from = stepMetaMap.get(sourceId);
                StepMeta to = stepMetaMap.get(targetId);

                if (from == null) {
                    log.warn("找不到源步骤: {}", sourceId);
                    continue;
                }
                if (to == null) {
                    log.warn("找不到目标步骤: {}", targetId);
                    continue;
                }

                // 默认启用连线
                boolean enabled = true;

                // 判断是错误步骤处理连线还是普通的连线
                Map<String, String> errorConfig = errorConfigMap.get(sourceId);
                if (errorConfig != null && targetId.equals(errorConfig.get("targetStep"))) {
                    // 错误处理连线
                    StepErrorMeta stepErrorMeta = new StepErrorMeta(new Variables(), from, to);
                    stepErrorMeta.setEnabled(true);
                    stepErrorMeta.setNrErrorsValuename(errorConfig.get("nrErrorsValuename"));
                    stepErrorMeta.setErrorDescriptionsValuename(errorConfig.get("errorDescriptionsValuename"));
                    from.setStepErrorMeta(stepErrorMeta);
                    to.setStepErrorMeta(stepErrorMeta);

                    TransHopMeta hop = new TransHopMeta(from, to, enabled);
                    transMeta.addTransHop(hop);
                    log.debug("创建错误处理连线: {} -> {}", from.getName(), to.getName());
                } else {
                    // 普通连线
                    TransHopMeta hop = new TransHopMeta(from, to, enabled);
                    transMeta.addTransHop(hop);
                    log.debug("创建普通连线: {} -> {}", from.getName(), to.getName());
                }
            }
        }

        // 8. 验证步骤连接
        if (validate && transMeta.getSteps().size() > 1 && transMeta.getTransHops().isEmpty()) {
            log.warn("转换有多个步骤但没有连线");
        }

        return transMeta;
    }

    /**
     * 解析错误处理的配置数据（适配新数据结构）
     *
     * @param data 步骤数据
     * @return 错误处理配置
     */
    private Map<String, String> getErrorHandlerConfig(JSONObject data) {
        Map<String, String> resultMap = new HashMap<>();

        // 是否启用错误处理
        boolean supportErrorType = data.getBooleanValue("supportErrorType");
        if (!supportErrorType) {
            return resultMap;
        }

        // 错误数列名
        String nrErrorsValuename = data.getString("nrErrorsValuename");
        // 错误描述列名
        String errorDescriptionsValuename = data.getString("errorDescriptionsValuename");
        // 目标步骤ID
        String targetStep = data.getString("targetStep");

        if (StringUtils.isNotBlank(targetStep)) {
            resultMap.put("nrErrorsValuename", nrErrorsValuename != null ? nrErrorsValuename : "");
            resultMap.put("errorDescriptionsValuename",
                    errorDescriptionsValuename != null ? errorDescriptionsValuename : "");
            resultMap.put("targetStep", targetStep);
        }

        return resultMap;
    }

    /**
     * 同步执行转换流
     *
     * @param transFlow  转换流
     * @param logChannel 日志通道
     */
    @Override
    public void syncExecute(TransFlow transFlow, LogChannel logChannel) {
        String config = transFlow.getConfig();
        TransWrapper trans = null;
        try {
            // 执行前强制用最新构造逻辑重建一次运行态 ktr，避免历史缓存的 stepId 导致 plugin missing
            refreshRuntimeKtr(transFlow);

            List<TransParam> params = new ArrayList<>();

            // 执行前置方法
            beforeTrans(transFlow.getId(), config, params);

            // 解析参数配置
            parseParamConfig(transFlow.getParamConfig(), params);

            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开始初始化开发流程！");

            // 执行转换
            trans = dataTransEngine.executeTrans(
                    transFlow,
                    params,
                    new KettleTransListener(LogChannelManager.getKey(logChannel.getType(), logChannel.getId())),
                    new KettleStepListener(LogChannelManager.getKey(logChannel.getType(), logChannel.getId())),
                    new KettleStepRowListener(LogChannelManager.getKey(logChannel.getType(), logChannel.getId())));

            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开发流程初始化成功！");
            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开始执行开发流程...");

            // 等待转换执行完成
            trans.waitUntilFinished();

            /*
             * 继续睡眠2秒，保证trans执行完后，继续步骤的后置动作
             */
            try {
                Thread.currentThread().sleep(2000);
            } catch (InterruptedException e) {
                log.warn("线程被中断", e);
                Thread.currentThread().interrupt();
            }

            /*
             * 根据配置分析组件，调用后置方法
             */
            afterTrans(transFlow.getId(), config);

        } catch (Exception e) {
            log.error("执行转换流程失败，转换流程id：{}", transFlow.getId(), e);
            logChannel.addLog(Constants.EXECUTE_STATUS.FAILURE, "执行流程失败。原因：" + e.getMessage());
        } finally {
            // 清理资源
            dataTransEngine.removeTrans(transFlow.getId());

            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "流程执行结束！");
            logChannel.setStatus(Constants.EXECUTE_STATUS.SUCCESS);
        }
    }

    /**
     * 强制刷新运行态 KTR（kettle_flow_repository.flow_content）
     * 目的：避免历史流程XML中的步骤ID与当前插件ID不一致，执行时报 plugin missing。
     */
    private void refreshRuntimeKtr(TransFlow transFlow) {
        if (transFlow == null) {
            throw new BusinessException("转换流程不能为空");
        }
        if (StringUtils.isBlank(transFlow.getConfig())) {
            throw new BusinessException("转换流程配置为空，请先保存流程配置");
        }
        try {
            TransMeta transMeta = buildTransMeta(transFlow.getId(), transFlow.getConfig(), false);
            transMeta.setName(transFlow.getId().toString());
            Integer rowSize = transFlow.getRowSize();
            if (rowSize == null || rowSize <= 0) {
                rowSize = 1000;
            }
            transMeta.setSizeRowset(rowSize);

            String bizType = transFlow.getStage() + "_" + Constants.TRANS;
            DataFlowRepository repository = DataFlowRepository.getRepository();
            TransFlowConfig exists = repository.getTrans(bizType, transFlow.getId().toString());
            if (exists == null) {
                TransFlowConfig runtimeConfig = new TransFlowConfig(
                        bizType,
                        transFlow.getId().toString(),
                        transMeta.getXML());
                runtimeConfig.setFlowJson(transFlow.getConfig());
                repository.saveTrans(runtimeConfig);
            } else {
                exists.setFlowContent(transMeta.getXML());
                exists.setFlowJson(transFlow.getConfig());
                repository.updateTrans(exists);
            }
        } catch (Exception e) {
            throw new BusinessException("刷新运行态KTR失败：" + e.getMessage());
        }
    }

    /**
     * 解析参数配置
     *
     * @param paramConfig 参数配置
     * @param params      参数列表
     */
    private void parseParamConfig(String paramConfig, List<TransParam> params) {
        if (StringUtils.isNotBlank(paramConfig)) {
            try {
                JSONArray objects = JSONArray.parseArray(paramConfig);
                if (null != objects) {
                    for (int i = 0; i < objects.size(); i++) {
                        JSONObject o = objects.getJSONObject(i);
                        String key = o.getString("key");
                        String value = o.getString("value");
                        if (StringUtils.isNotBlank(key)) {
                            TransParam transParam = new TransParam(key, value, "", "");
                            params.add(transParam);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("解析参数配置失败", e);
            }
        }
    }

    /**
     * 执行转换前置方法
     *
     * @param flowId 流程ID
     * @param config 配置信息
     * @param params 参数列表
     */
    @Override
    public void beforeTrans(Integer flowId, String config, List<TransParam> params) {
        if (flowId == null || StringUtils.isBlank(config) || params == null) {
            log.debug("前置方法跳过：flowId={}, config空={}, params空={}", flowId, StringUtils.isBlank(config), params == null);
            return;
        }

        forEachStep(config, (stepId, stepType, dataStr) -> {
            AbstractStepMetaConstructor constructor = StepMetaConstructorFactory.getConstructor(stepType);
            constructor.beforeStep(flowId, stepId, dataStr, params);
        });
    }

    /**
     * 执行转换后置方法
     *
     * @param flowId 流程ID
     * @param config 配置信息
     */
    private void afterTrans(Integer flowId, String config) {
        if (flowId == null || StringUtils.isBlank(config)) {
            log.debug("后置方法跳过：flowId={}, config空={}", flowId, StringUtils.isBlank(config));
            return;
        }

        forEachStep(config, (stepId, stepType, dataStr) -> {
            AbstractStepMetaConstructor constructor = StepMetaConstructorFactory.getConstructor(stepType);
            constructor.afterStep(flowId, stepId, dataStr);
        });
    }

    /**
     * 遍历配置中的步骤节点，并对每个步骤执行指定的处理函数
     *
     * @param config      前端配置 JSON 字符串
     * @param stepHandler 步骤处理函数，参数为 (stepId, stepType, dataStr)
     */
    private void forEachStep(String config, StepHandler stepHandler) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(config);
            JSONArray cells = jsonObject.getJSONArray("cells");
            if (cells == null || cells.isEmpty()) {
                log.debug("配置中无 cells 节点或为空");
                return;
            }

            for (int i = 0; i < cells.size(); i++) {
                JSONObject cell = cells.getJSONObject(i);
                String shape = cell.getString("shape");
                // 跳过连线
                if ("edge".equals(shape)) {
                    continue;
                }

                JSONObject data = cell.getJSONObject("data");
                if (data == null) {
                    log.debug("步骤节点缺少 data 字段，cell id: {}", cell.getString("id"));
                    continue;
                }

                String stepType = data.getString("code");
                if (StringUtils.isBlank(stepType)) {
                    log.debug("步骤 data 中缺少 code 字段，cell id: {}", cell.getString("id"));
                    continue;
                }

                String stepId = cell.getString("id");
                String dataStr = data.toJSONString();

                // 执行步骤处理函数
                stepHandler.handle(stepId, stepType, dataStr);
            }
        } catch (Exception e) {
            log.error("遍历步骤配置失败", e);
            throw new BusinessException("遍历步骤配置失败：" + e.getMessage());
        }
    }

    /**
     * 步骤处理函数接口
     */
    @FunctionalInterface
    private interface StepHandler {
        void handle(String stepId, String stepType, String dataStr);
    }

    /**
     * 预览转换流程数据
     *
     * @param form 转换流程配置表单
     * @return 预览数据
     */
    @Override
    public PreviewVo preview(PreviewForm form) {

        PreviewVo previewVo = new PreviewVo();

        String config = form.getConfig();
        JSONObject jsonObject = JSONObject.parseObject(config);
        // 组件code
        String code = jsonObject.getString("code");

        TransMeta transMeta = new TransMeta();
        transMeta.setName("转换");
        PluginRegistry registryID = PluginRegistry.getInstance();

        AbstractStepMetaConstructor constructor = StepMetaConstructorFactory.getConstructor(code);
        List<TransParam> params = new ArrayList<>();
        constructor.beforeStep(form.getId(), null, form.getConfig(), params);
        setParams(form.getId(), transMeta, params);

        StepContext context = new StepContext();
        context.setRegistryID(registryID);
        context.setStepMetaMap(new HashMap<>());
        context.setFlowId(form.getId());
        context.setValidate(true);

        StepMeta stepMeta = constructor.create(config, transMeta, context);
        stepMeta.setDraw(false);
        transMeta.addStep(stepMeta);
        RowMetaInterface r;
        try {
            r = transMeta.getThisStepFields(stepMeta.getName(), new RowMeta());
            previewVo.setFieldList(r.getFieldNames());
            r.getFieldNamesAndTypes(1);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }
        List<Object[]> previewData = getPreviewData(stepMeta, transMeta);
        for (int i = 0; i < previewData.size(); i++) {
            Object[] objects = previewData.get(i);
            for (int j = 0; j < objects.length; j++) {
                ValueMetaInterface valueMeta = r.getValueMeta(j);

                String show;
                try {
                    if (null == valueMeta) {
                        continue;
                    } else if (objects[j] == null) {
                        show = "";
                    } else {
                        show = valueMeta.getString(objects[j]);
                    }
                } catch (KettleValueException e) {
                    log.error("", e);
                    throw new BusinessException(objects[j] + "转换失败！");
                }
                objects[j] = show;
            }
        }
        previewVo.setDataList(previewData);

        return previewVo;

    }

    /**
     * 设置转换流程参数
     *
     * @param flowId    转换流程ID
     * @param transMeta 转换流程元数据
     * @param params    转换参数列表
     */
    @Override
    public void setParams(Integer flowId, TransMeta transMeta, List<TransParam> params) {
        if (null == flowId) {
            return;
        }
        TransFlow transFlow = transFlowMapper.selectById(flowId);

        if (CollectionUtils.isNotEmpty(params)) {
            for (TransParam param : params) {
                try {
                    transMeta.addParameterDefinition(param.getName(), param.getValue(), "");
                } catch (DuplicateParamException e) {
                    e.printStackTrace();
                }
            }
        }

        String paramConfig = transFlow.getParamConfig();
        if (StringUtils.isNotBlank(paramConfig)) {
            JSONArray objects = JSONArray.parseArray(paramConfig);
            if (CollectionUtils.isNotEmpty(objects)) {
                for (int i = 0; i < objects.size(); i++) {
                    JSONObject o = objects.getJSONObject(i);
                    try {
                        transMeta.addParameterDefinition(o.getString("key"), o.getString("value"), "");
                    } catch (DuplicateParamException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        transMeta.activateParameters();
    }

    /**
     * 获取预览数据
     *
     * @param step
     * @param transMeta
     */
    public List<Object[]> getPreviewData(StepMeta step, TransMeta transMeta) {
        StepMetaInterface stepMetaInterface = step.getStepMetaInterface();
        TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(transMeta, stepMetaInterface,
                step.getName());
        int previewSize = 1000;// 默认预览1000条数据
        TransWrapper trans = new TransWrapper(previewMeta);
        trans.setPreview(true);

        // 准备执行转换
        try {
            trans.prepareExecution(null);
        } catch (final KettleException e) {
            trans.stopAll();
            KettleLogStore.discardLines(trans.getLogChannelId(), true);
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }

        // Add the preview / debugging information...
        //
        TransDebugMeta transDebugMeta = new TransDebugMeta(previewMeta);
        String[] previewStepNames = new String[]{step.getName()};
        for (int i = 0; i < previewStepNames.length; i++) {
            StepMeta stepMeta = transMeta.findStep(previewStepNames[i]);
            StepDebugMeta stepDebugMeta = new StepDebugMeta(stepMeta);
            stepDebugMeta.setReadingFirstRows(true);
            stepDebugMeta.setRowCount(previewSize);
            transDebugMeta.getStepDebugMetaMap().put(stepMeta, stepDebugMeta);
        }

        final List<String> previewComplete = new ArrayList<String>();

        transDebugMeta.addBreakPointListers(new BreakPointListener() {
            @Override
            public void breakPointHit(TransDebugMeta transDebugMeta, StepDebugMeta stepDebugMeta,
                                      RowMetaInterface rowBufferMeta, List<Object[]> rowBuffer) {
                String stepName = stepDebugMeta.getStepMeta().getName();
                previewComplete.add(stepName);
            }
        });

        transDebugMeta.addRowListenersToTransformation(trans);

        // 执行转换
        try {
            trans.startThreads();
        } catch (final KettleException e) {
            trans.stopAll();
            KettleLogStore.discardLines(trans.getLogChannelId(), true);
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }

        while (previewComplete.size() < previewStepNames.length
                && !trans.isFinished()) {// 此处需要加上停止标志，否则可能一直死循环
            // How many rows are done?
            int nrDone = 0;
            int nrTotal = 0;
            for (StepDebugMeta stepDebugMeta : transDebugMeta.getStepDebugMetaMap().values()) {
                nrDone += stepDebugMeta.getRowBuffer().size();
                nrTotal += stepDebugMeta.getRowCount();
            }

            int pct = 100 * nrDone / nrTotal;
            // System.out.println("处理进度：" + pct);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // Ignore errors
            }

        }
        trans.stopAll();
        KettleLogStore.discardLines(trans.getLogChannelId(), true);

        // 获取数据
        for (StepMeta stepMeta : transDebugMeta.getStepDebugMetaMap().keySet()) {
            if (stepMeta.getName().equals(step.getName())) {
                StepDebugMeta stepDebugMeta = transDebugMeta.getStepDebugMetaMap().get(stepMeta);
                return stepDebugMeta.getRowBuffer();
            }
        }

        return null;
    }

    /**
     * 获取流字段，编辑字段，下拉框使用
     *
     * @param flowId
     * @param config
     * @param stepName
     * @param type
     * @return
     */
    @Override
    public String[] getFieldStream(Integer flowId, String config, String stepName, Integer type) {
        if (StringUtils.isBlank(config)) {
            return null;
        }
        TransMeta transMeta = buildTransMeta(flowId, config, false);
        List<TransParam> params = new ArrayList<>();
        beforeTrans(flowId, config, params);
        setParams(flowId, transMeta, params);
        try {
            RowMetaInterface r = transMeta.getPrevStepFields(stepName);
            if (null == type) {
                return r.getFieldNames();
            } else {
                List<ValueMetaInterface> valueMetaList = r.getValueMetaList();
                List<String> fields = new ArrayList<>();
                for (ValueMetaInterface valueMetaInterface : valueMetaList) {
                    if (type == valueMetaInterface.getType()) {
                        fields.add(valueMetaInterface.getName());
                    }
                }
                return fields.toArray(new String[0]);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return new String[0];
    }

    /**
     * 调试方法：解析并打印前端配置
     */
    @Override
    public void debugParseConfig(String config) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(config);
            log.info("=== 开始解析前端配置 ===");

            JSONArray cells = jsonObject.getJSONArray("cells");
            log.info("cells数量: {}", cells.size());

            int stepCount = 0;
            int edgeCount = 0;

            for (int i = 0; i < cells.size(); i++) {
                JSONObject cell = cells.getJSONObject(i);
                String shape = cell.getString("shape");
                String id = cell.getString("id");

                if ("edge".equals(shape)) {
                    edgeCount++;
                    JSONObject source = cell.getJSONObject("source");
                    JSONObject target = cell.getJSONObject("target");
                    log.info("连线[{}]: {} -> {}", id,
                            source != null ? source.getString("cell") : "null",
                            target != null ? target.getString("cell") : "null");
                } else {
                    stepCount++;
                    JSONObject data = cell.getJSONObject("data");
                    JSONObject position = cell.getJSONObject("position");
                    log.info("步骤[{}]: shape={}, name={}, type={}, position=({},{})",
                            id,
                            shape,
                            data != null ? data.getString("name") : "null",
                            data != null ? data.getString("type") : "null",
                            position != null ? position.getInteger("x") : "null",
                            position != null ? position.getInteger("y") : "null");
                }
            }

            log.info("解析完成: 共{}个步骤, {}个连线", stepCount, edgeCount);

        } catch (Exception e) {
            log.error("解析配置失败", e);
        }
    }

    /**
     * 校验转换流程运行状态
     *
     * @param id 转换流id
     * @return 转换流程运行状态
     */
    @Override
    public Boolean checkTransStatus(Integer id) {
        return dataTransEngine.checkTransStatus(id);
    }

    /**
     * 获取转换流程运行日志
     *
     * @param id 转换流id
     * @return 转换流程运行日志
     */
    @Override
    public LogChannel getProcessLog(Integer id) {

        // 获取日志通道
        String key = LogChannelManager.getKey(DataTransEngine.ResourceType.TRANS.name(), String.valueOf(id));
        LogChannel logChannel = LogChannelManager.get(key);
        if (logChannel == null) {
            throw new BusinessException("当前会话已超时，回放条件失效，请重新发起！");// todo
        }

        return logChannel.clone();
    }

    /**
     * 停止转换流程
     *
     * @param id 转换流程id
     */
    @Override
    public void stop(Integer id) {
        // 停止转换流程
        dataTransEngine.stopTrans(id);
    }

    /**
     * 展示转换流程图片
     *
     * @param transFlow 转换流程对象
     * @return 转换流程图片base64编码
     */
    @Override
    public String showTransImg(TransFlow transFlow) {
        // 参数验证
        if (transFlow == null) {
            throw new BusinessException("转换流程不能为空");
        }

        try {
            return dataTransEngine.getBase64TransImage(transFlow.getId(), transFlow.getStage());
        } catch (Exception e) {
            log.error("获取转换流程图片失败，转换流程id：{}", transFlow.getId(), e);
            throw new BusinessException("获取转换流程图片失败：" + e.getMessage());
        }
    }
}
