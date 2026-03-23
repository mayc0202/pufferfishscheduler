package com.pufferfishscheduler.master.collect.trans.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.entity.TransGroup;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.domain.form.collect.PreviewForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.domain.vo.collect.PreviewVo;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;
import com.pufferfishscheduler.master.collect.group.service.TransGroupService;
import com.pufferfishscheduler.master.collect.trans.engine.DataFlowRepository;
import com.pufferfishscheduler.master.collect.trans.engine.DataTransEngine;
import com.pufferfishscheduler.master.collect.trans.engine.DataTransEngine.ResourceType;
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
import com.pufferfishscheduler.master.collect.trans.service.TransFlowService;

import lombok.extern.slf4j.Slf4j;

/**
 * 转换流实现类
 *
 * @author Mayc
 * @since 2026-03-04 17:09
 */
@Slf4j
@Service
public class TransFlowServiceImpl implements TransFlowService {

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private TransGroupService transGroupService;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    @Autowired
    DataTransEngine dataTransEngine;

    /**
     * 分页查询转换流列表
     *
     * @param groupId
     * @param flowName
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public IPage<TransFlowVo> list(Integer groupId, String flowName, Integer pageNo, Integer pageSize) {
        // 1. 构建分页对象（确保页码/页大小合法）
        Page<TransFlow> pageParam = new Page<>(pageNo, pageSize);

        // 2. 构建查询条件并执行分页查询
        LambdaQueryWrapper<TransFlow> ldq = buildQueryWrapper(groupId, flowName);
        Page<TransFlow> page = transFlowMapper.selectPage(pageParam, ldq);

        // 3. 空结果处理（关键优化：直接拷贝所有分页属性）
        Page<TransFlowVo> result = new Page<>();
        BeanUtils.copyProperties(page, result);

        if (CollectionUtils.isEmpty(page.getRecords())) {
            result.setRecords(Collections.emptyList());
            return result;
        }

        // 4. 提取所有用到的分组ID
        Set<Integer> groupIds = page.getRecords().stream()
                .map(TransFlow::getGroupId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        // 5. 批量获取分组全路径映射（性能更优）
        Map<Integer, String> groupFullPathMap = transGroupService.getGroupFullPathMap(groupIds);

        // 6. 转换VO（保证转换后的记录数与原记录数一致）
        List<TransFlowVo> transFlowList = page.getRecords().stream()
                .map(transFlow -> convertTransFlowVo(transFlow, groupFullPathMap))
                .collect(Collectors.toList());

        // 7. 设置转换后的记录列表
        result.setRecords(transFlowList);

        return result;
    }

    /**
     * 转换分组（父子）+ 未删除的转换流程树
     *
     * @param name 分组名称模糊条件，与 {@link TransGroupService#getGroupList} 一致；可为空表示全部分组
     */
    @Override
    public List<Tree> tree(String name) {
        List<TransGroup> groups = transGroupService.getGroupList(name);
        if (CollectionUtils.isEmpty(groups)) {
            return Collections.emptyList();
        }

        Set<Integer> groupIds = groups.stream().map(TransGroup::getId).collect(Collectors.toSet());

        LambdaQueryWrapper<TransFlow> flowQuery = new LambdaQueryWrapper<>();
        flowQuery.eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);
        flowQuery.in(TransFlow::getGroupId, groupIds);
        List<TransFlow> flows = transFlowMapper.selectList(flowQuery);

        List<Tree> allNodes = new ArrayList<>();
        for (TransGroup group : groups) {
            Tree node = new Tree();
            BeanUtils.copyProperties(group, node);
            node.setType(Constants.TREE_TYPE.GROUP);
            node.setOrderBy(group.getOrderBy());
            allNodes.add(node);
        }
        for (TransFlow flow : flows) {
            Tree node = new Tree();
            node.setId(flow.getId());
            node.setName(flow.getName());
            node.setParentId(flow.getGroupId());
            node.setType(Constants.TREE_TYPE.TRANS_FLOW);
            node.setOrderBy(flow.getId());
            allNodes.add(node);
        }

        return treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));
    }

    /**
     * 构建查询条件
     *
     * @param groupId
     * @param name
     * @return
     */
    private LambdaQueryWrapper<TransFlow> buildQueryWrapper(Integer groupId, String name) {
        LambdaQueryWrapper<TransFlow> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (Objects.nonNull(groupId)) {
            // 获取该分组及其所有子分组的ID列表
            List<Integer> groupIds = transGroupService.getAllChildGroupIds(groupId);
            // 查询这些分组下的所有流程
            queryWrapper.in(TransFlow::getGroupId, groupIds);
        }

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(TransFlow::getName, name);
        }

        return queryWrapper;
    }

    /**
     * 将转换流程信息为Vo
     *
     * @param transFlow        转换流实体
     * @param groupFullPathMap 分组ID-全路径映射
     * @return 转换流VO
     */
    private TransFlowVo convertTransFlowVo(TransFlow transFlow, Map<Integer, String> groupFullPathMap) {
        TransFlowVo vo = new TransFlowVo();
        BeanUtils.copyProperties(transFlow, vo);
        // 获取全路径名称
        vo.setGroupName(groupFullPathMap.getOrDefault(vo.getGroupId(), ""));
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));
        return vo;
    }

    /**
     * 添加转换流
     *
     * @param flow  转换流表单
     * @param stage 数据处理阶段
     * @return 转换流id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public Integer addFlow(TransFlowForm flow, String stage) {

        // 校验相同分组下转换流名称是否存在
        verifyTransFlowNameExisted(flow.getName(), flow.getGroupId(), null);

        // 转换流入库
        TransFlow transFlow = new TransFlow();
        transFlow.setStage(stage);
        BeanUtils.copyProperties(flow, transFlow);
        transFlow.setDeleted(Constants.DELETE_FLAG.FALSE);
        transFlow.setCreatedBy(UserContext.getCurrentAccount());
        transFlow.setCreatedTime(new Date());
        transFlowMapper.insert(transFlow);

        return transFlow.getId();
    }

    /**
     * 更新转换流
     *
     * @param flow
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void updateFlow(TransFlowForm flow) {

        // 校验转换流是否存在
        TransFlow transFlow = getTransFlowById(flow.getId());

        // 校验相同分组下转换流名称是否存在
        verifyTransFlowNameExisted(flow.getName(), flow.getGroupId(), flow.getId());

        BeanUtils.copyProperties(flow, transFlow);
        transFlow.setUpdatedBy(UserContext.getCurrentAccount());
        transFlow.setUpdatedTime(new Date());
        transFlowMapper.updateById(transFlow);
    }

    /**
     * 删除转换流
     *
     * @param id 转换流id
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void deleteFlow(Integer id) {
        // 校验转换流是否存在
        TransFlow transFlow = getTransFlowById(id);

        // TODO 后续校验转换流是否被引用

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<TransFlow> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        transFlowMapper.update(null, updateWrapper);

        // 删除转换流数据
        DataFlowRepository repository = DataFlowRepository.getRepository();
        repository.deleteTrans(transFlow.getStage() + "_" + Constants.TRANS, transFlow.getId().toString());
    }

    /**
     * 查询转换流详情
     *
     * @param id 转换流id
     * @return
     */
    @Override
    public TransFlowVo detail(Integer id) {
        TransFlow transFlow = getTransFlowById(id);
        TransFlowVo transFlowVo = new TransFlowVo();
        BeanUtils.copyProperties(transFlow, transFlowVo);
        return transFlowVo;
    }

    /**
     * 校验相同分组下转换流名称是否存在
     *
     * @param name
     * @param groupId
     */
    private void verifyTransFlowNameExisted(String name, Integer groupId, Integer id) {
        LambdaQueryWrapper<TransFlow> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransFlow::getName, name)
                .eq(TransFlow::getGroupId, groupId)
                .eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (Objects.nonNull(id)) {
            ldq.notIn(TransFlow::getId, id);
        }

        TransFlow transFlow = transFlowMapper.selectOne(ldq);
        if (Objects.nonNull(transFlow)) {
            throw new BusinessException("当前分组下流程名称已存在!");
        }
    }

    /**
     * 查询转换流
     *
     * @param id
     * @return
     */
    private TransFlow getTransFlowById(Integer id) {
        // 查询转换流
        LambdaQueryWrapper<TransFlow> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransFlow::getId, id)
                .eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);
        TransFlow transFlow = transFlowMapper.selectOne(ldq);
        // 校验转换流是否存在
        if (Objects.isNull(transFlow)) {
            throw new BusinessException("请校验转换流程是否存在!");
        }

        return transFlow;
    }

    /**
     * 校验转换流是否存在
     *
     * @param id
     * @return
     */
    private void verifyTransFlowExist(Integer id) {
        // 查询转换流
        LambdaQueryWrapper<TransFlow> ldq = new LambdaQueryWrapper<>();
        ldq.eq(TransFlow::getId, id)
                .eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);
        TransFlow transFlow = transFlowMapper.selectOne(ldq);
        if (Objects.isNull(transFlow)) {
            throw new BusinessException("请校验转换流程是否存在!");
        }
    }

    /**
     * 设置转换流配置
     *
     * @param form 转换流配置表单
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void setConfig(TransFlowConfigForm form) {
        // 参数验证
        if (form == null) {
            throw new BusinessException("配置表单不能为空");
        }
        if (form.getId() == null) {
            throw new BusinessException("转换流程ID不能为空");
        }
        // 如果转换配置是空的，直接返回
        if (StringUtils.isBlank(form.getConfig())) {
            throw new BusinessException("转换流程配置不能为空");
        }

        // 添加调试
        log.info("接收到的配置: {}", form.getConfig());
        debugParseConfig(form.getConfig());

        // 查询转换流程
        TransFlow transFlow = getTransFlowById(form.getId());

        try {
            // 1. 生成设计器流程
            TransMeta transMeta = buildTransMeta(form.getId(), form.getConfig(), true);
            transMeta.setName(transFlow.getId().toString());
            // 设置批处理大小，如果为null则使用默认值1000
            Integer rowSize = transFlow.getRowSize();
            if (rowSize == null || rowSize <= 0) {
                rowSize = 1000; // 设置更大的默认值
            }
            transMeta.setSizeRowset(rowSize);

            // 2. 将生成的设计器流程并保存到设计库中
            String bizType = transFlow.getStage() + "_" + Constants.TRANS;
            DataFlowRepository repository = DataFlowRepository.getRepository();
            TransFlowConfig efcTrans = repository.getTrans(bizType,
                    transFlow.getId().toString());

            if (null == efcTrans) {
                efcTrans = new TransFlowConfig(
                        bizType,
                        transFlow.getId().toString(),
                        transMeta.getXML());
                repository.saveTrans(efcTrans);
            } else {
                efcTrans = new TransFlowConfig(
                        bizType,
                        transFlow.getId().toString(),
                        transMeta.getXML());
                repository.updateTrans(efcTrans);
            }

            // 3. 保存开发流程配置信息
            transFlow.setConfig(form.getConfig());
            transFlow.setUpdatedBy(UserContext.getCurrentAccount());
            transFlow.setUpdatedTime(new Date());
            transFlowMapper.updateById(transFlow);
        } catch (Exception e) {
            log.error("保存转换流程配置失败，转换流程id：{}", transFlow.getId(), e);
            throw new BusinessException("保存转换流程配置失败：" + e.getMessage());
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
    public TransMeta buildTransMeta(Integer flowId, String config, boolean validate) {
        // 参数验证
        if (flowId == null) {
            throw new BusinessException("流程ID不能为空");
        }
        if (StringUtils.isBlank(config)) {
            throw new BusinessException("配置信息不能为空");
        }

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
            if (stepMetaConstructor == null) {
                throw new BusinessException("不支持的步骤类型: " + type);
            }

            StepContext context = new StepContext();
            context.setRegistryID(registryID);
            context.setStepMetaMap(stepMetaMap);
            context.setId(id);
            context.setValidate(validate);
            context.setFlowId(flowId);
            context.setStepNames(stepNames);

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
     * 执行转换流
     *
     * @param id 转换流id
     * @return
     */
    @Override
    public void execute(Integer id) {
        TransFlow transFlow = getTransFlowById(id);

        LogChannel logChannel = new LogChannel(ResourceType.TRANS.name(), id + "", transFlow.getName());
        LogChannelManager.put(LogChannelManager.getKey(logChannel.getType(), logChannel.getId()), logChannel);
        logChannel.setStatus(Constants.EXECUTE_STATUS.RUNNING);

        Thread thread = new Thread(new Runnable() {
            public void run() {
                syncExecute(transFlow, logChannel);
            }
        });

        thread.start();
    }

    /**
     * 同步执行转换流
     *
     * @param transFlow  转换流
     * @param logChannel 日志通道
     */
    private void syncExecute(TransFlow transFlow, LogChannel logChannel) {
        String config = transFlow.getConfig();
        TransWrapper trans = null;
        try {
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
    public void beforeTrans(Integer flowId, String config, List<TransParam> params) {
        // 参数验证
        if (flowId == null) {
            log.warn("流程ID为空，跳过前置方法执行");
            return;
        }
        if (StringUtils.isBlank(config)) {
            log.warn("配置信息为空，跳过前置方法执行");
            return;
        }
        if (params == null) {
            log.warn("参数列表为空，跳过前置方法执行");
            return;
        }

        try {
            /*
             * 1. 获取步骤列表
             */
            JSONObject jsonObject = JSONObject.parseObject(config);
            JSONArray steps = jsonObject.getJSONArray("steps");
            if (null != steps) {
                for (int i = 0; i < steps.size(); i++) {
                    JSONObject jo = steps.getJSONObject(i);
                    String shape = jo.getString("shape");
                    if (StringUtils.isNotBlank(shape)) {
                        AbstractStepMetaConstructor stepMetaConstructor = StepMetaConstructorFactory
                                .getConstructor(shape);
                        if (stepMetaConstructor != null) {
                            try {
                                stepMetaConstructor.beforeStep(flowId, jo.getString("id"), jo.getString("data"),
                                        params);
                            } catch (Exception e) {
                                log.warn("执行步骤前置方法失败，流程ID：{}, 步骤类型：{}", flowId, shape, e);
                            }
                        } else {
                            log.warn("未找到步骤构造器，步骤类型：{}", shape);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("执行前置方法失败，流程ID：{}", flowId, e);
        }
    }

    /**
     * 执行转换后置方法
     * 
     * @param flowId 流程ID
     * @param config 配置信息
     */
    private void afterTrans(Integer flowId, String config) {
        // 参数验证
        if (flowId == null) {
            log.warn("流程ID为空，跳过后置方法执行");
            return;
        }
        if (StringUtils.isBlank(config)) {
            log.warn("配置信息为空，跳过后置方法执行");
            return;
        }

        try {
            /*
             * 1. 获取步骤列表
             */
            JSONObject jsonObject = JSONObject.parseObject(config);
            JSONArray steps = jsonObject.getJSONArray("steps");
            if (null != steps) {
                for (int i = 0; i < steps.size(); i++) {
                    JSONObject jo = steps.getJSONObject(i);
                    String shape = jo.getString("shape");
                    if (StringUtils.isNotBlank(shape)) {
                        AbstractStepMetaConstructor stepMetaConstructor = StepMetaConstructorFactory
                                .getConstructor(shape);
                        if (stepMetaConstructor != null) {
                            try {
                                stepMetaConstructor.afterStep(flowId, jo.getString("id"), jo.getString("data"));
                            } catch (Exception e) {
                                log.warn("执行步骤后置方法失败，流程ID：{}, 步骤类型：{}", flowId, shape, e);
                            }
                        } else {
                            log.warn("未找到步骤构造器，步骤类型：{}", shape);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("执行后置方法失败，流程ID：{}", flowId, e);
        }
    }

    /**
     * 停止转换流程
     * 
     * @param id 转换流程id
     */
    @Override
    public void stop(Integer id) {
        // 校验转换流程是否存在
        verifyTransFlowExist(id);

        // 停止转换流程
        dataTransEngine.stopTrans(id);
    }

    /**
     * 展示转换流程图片
     * 
     * @param id 转换流程id
     * @return 转换流程图片base64编码
     */
    @Override
    public String showTransImg(Integer id) {
        // 参数验证
        if (id == null) {
            throw new BusinessException("转换流程ID不能为空");
        }

        TransFlow transFlow = getTransFlowById(id);
        if (null == transFlow || transFlow.getDeleted()) {
            throw new BusinessException("数据流程不存在，请刷新重试！");
        }

        try {
            return dataTransEngine.getBase64TransImage(transFlow.getId(), transFlow.getStage());
        } catch (Exception e) {
            log.error("获取转换流程图片失败，转换流程id：{}", id, e);
            throw new BusinessException("获取转换流程图片失败：" + e.getMessage());
        }
    }

    /**
     * 复制转换流程
     * 
     * @param id 转换流程id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void copyTrans(Integer id) {
        // 参数验证
        if (id == null) {
            throw new BusinessException("转换流程ID不能为空");
        }

        TransFlow transFlow = getTransFlowById(id);

        // 创建复制的转换流程
        TransFlow copyTrans = new TransFlow();
        BeanUtils.copyProperties(transFlow, copyTrans, "id", "name", "path", "createdBy", "createdTime", "updatedBy",
                "updatedTime");
        copyTrans.setName(transFlow.getName() + "_副本");
        copyTrans.setCreatedBy(UserContext.getCurrentAccount());
        copyTrans.setCreatedTime(new Date());
        transFlowMapper.insert(copyTrans);

        // 复制配置
        if (StringUtils.isNotBlank(copyTrans.getConfig())) {
            TransFlowConfigForm flowConfigForm = new TransFlowConfigForm();
            flowConfigForm.setId(copyTrans.getId());
            flowConfigForm.setConfig(copyTrans.getConfig());
            flowConfigForm.setImg(copyTrans.getImage());
            try {
                setConfig(flowConfigForm);
            } catch (Exception e) {
                log.error("创建流程失败，原流程id：{}", id, e);
                throw new BusinessException("创建流程失败：" + e.getMessage());
            }
        }
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
        // context.setRootPath(rootPath);
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
        String[] previewStepNames = new String[] { step.getName() };
        for (int i = 0; i < previewStepNames.length; i++) {
            StepMeta stepMeta = transMeta.findStep(previewStepNames[i]);
            StepDebugMeta stepDebugMeta = new StepDebugMeta(stepMeta);
            stepDebugMeta.setReadingFirstRows(true);
            stepDebugMeta.setRowCount(previewSize);
            transDebugMeta.getStepDebugMetaMap().put(stepMeta, stepDebugMeta);
        }

        final List<String> previewComplete = new ArrayList<String>();
        // We add a break-point that is called every time we have a step with a full
        // preview row buffer
        // That makes it easy and fast to see if we have all the rows we need
        //
        transDebugMeta.addBreakPointListers(new BreakPointListener() {
            @Override
            public void breakPointHit(TransDebugMeta transDebugMeta, StepDebugMeta stepDebugMeta,
                    RowMetaInterface rowBufferMeta, List<Object[]> rowBuffer) {
                String stepName = stepDebugMeta.getStepMeta().getName();
                previewComplete.add(stepName);
            }
        });

        // set the appropriate listeners on the transformation...
        //
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
     * 复制转换流
     * 
     * @param id 转换流id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void copy(Integer id) {
        // 获取转换流程
        TransFlow transFlow = getTransFlowById(id);

        TransFlow copyTrans = new TransFlow();
        BeanUtils.copyProperties(transFlow, copyTrans, "id", "name", "path", "createdBy", "createdTime", "updatedBy",
                "updatedTime");
        copyTrans.setName(transFlow.getName() + "_副本");
        copyTrans.setCreatedBy(UserContext.getCurrentAccount());
        copyTrans.setCreatedTime(new Date());
        transFlowMapper.insert(copyTrans);

        // 复制配置
        if (StringUtils.isNotBlank(copyTrans.getConfig())) {
            TransFlowConfigForm form = new TransFlowConfigForm();
            form.setId(copyTrans.getId());
            form.setConfig(copyTrans.getConfig());
            form.setImg(copyTrans.getImage());
            // 复制配置
            setConfig(form);
        }
    }

    /**
     * 获取转换流程运行日志
     * 
     * @param id 转换流id
     * @return 转换流程运行日志
     */
    @Override
    public LogChannel getProcessLog(Integer id) {
        // 校验转换流程是否存在
        verifyTransFlowExist(id);

        // 获取日志通道
        String key = LogChannelManager.getKey(ResourceType.TRANS.name(), String.valueOf(id));
        LogChannel logChannel = LogChannelManager.get(key);
        if (logChannel == null) {
            throw new BusinessException("当前会话已超时，回放条件失效，请重新发起！");// todo
        }

        return logChannel.clone();
    }
}
