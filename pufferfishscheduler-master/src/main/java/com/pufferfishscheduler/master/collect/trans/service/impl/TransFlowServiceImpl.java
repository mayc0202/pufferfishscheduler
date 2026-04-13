package com.pufferfishscheduler.master.collect.trans.service.impl;

import java.util.*;
import java.util.stream.Collectors;

import com.pufferfishscheduler.master.collect.trans.service.StepService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;
import com.pufferfishscheduler.master.collect.group.service.TransGroupService;
import com.pufferfishscheduler.trans.engine.DataFlowRepository;
import com.pufferfishscheduler.trans.engine.DataTransEngine.ResourceType;
import com.pufferfishscheduler.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannel;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannelManager;
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
    private StepService stepService;

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

        // 监听是否是清洗组件
        if (transFlowVo.getConfig() != null) {
            String updateConfig = stepService.updateDataCleanRuleConfig(transFlowVo.getConfig());
            transFlowVo.setConfig(updateConfig);
        }

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
        stepService.debugParseConfig(form.getConfig());

        // 查询转换流程
        TransFlow transFlow = getTransFlowById(form.getId());

        try {
            // 1. 生成设计器流程
            TransMeta transMeta = stepService.buildTransMeta(form.getId(), form.getConfig(), true);
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
                efcTrans.setFlowJson(form.getConfig());
                repository.saveTrans(efcTrans);
            } else {
                efcTrans.setFlowContent(transMeta.getXML());
                efcTrans.setFlowJson(form.getConfig());
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
                stepService.syncExecute(transFlow, logChannel);
            }
        });

        thread.start();
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
        stepService.stop(id);
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

}
