package com.pufferfishscheduler.master.database.metadata.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.DatabaseCategory;
import com.pufferfishscheduler.common.enums.Enable;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.dao.entity.MetadataTask;
import com.pufferfishscheduler.dao.mapper.MetadataTaskMapper;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskForm;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskUpdateForm;
import com.pufferfishscheduler.domain.vo.metadata.MetadataTaskVo;
import com.pufferfishscheduler.master.database.database.service.DbBasicService;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.database.service.DbGroupService;
import com.pufferfishscheduler.master.database.metadata.service.MetadataService;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.master.dispatch.kafka.TaskDispatchKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;


/**
 * 元数据业务层实现类
 *
 * @author Mayc
 * @since 2025-11-17  17:31
 */
@Slf4j
@Service("metadataService")
public class MetadataServiceImpl implements MetadataService {

    @Autowired
    private DictService dictService;

    @Autowired
    private DbGroupService dbGroupService;

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private DbBasicService basicService;

    @Autowired
    private MetadataTaskMapper metadataTaskMapper;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    @Autowired
    private TaskDispatchKafkaProducer taskDispatchKafkaProducer;

    /**
     * 任务卡住阈值（用于“手动立即触发”的抢占）
     * <p>
     * 与调度侧保持同名配置，避免行为不一致。
     */
    @Value("${scheduler.metadata.stuck-threshold-ms:300000}")
    private long stuckThresholdMs;

    /**
     * 获取元数据分组树形结构
     *
     * @param name
     * @return
     */
    @Override
    public List<Tree> tree(String name) {
        // 查询分组
        List<DbGroup> groups = dbGroupService.getGroupList(name);
        if (CollectionUtils.isEmpty(groups)) {
            return Collections.emptyList();
        }

        // 整合分组id
        Set<Integer> groupIds = groups.stream()
                .map(DbGroup::getId)
                .collect(Collectors.toSet());

        // 查询数据源
        List<DbDatabase> databaseList = dbDatabaseService.listDatabasesByGroupIds(groupIds, DatabaseCategory.SQL.getCode());
        if (CollectionUtils.isEmpty(databaseList)) {
            return Collections.emptyList();
        }

        // 整合数据源id
        Set<Integer> databaseIds = databaseList.stream()
                .map(DbDatabase::getId)
                .collect(Collectors.toSet());

        // 转成map
        Map<Integer, DbDatabase> dbMap = databaseList.stream().collect(Collectors.toMap(DbDatabase::getId, database -> database));

        LambdaQueryWrapper<MetadataTask> queryWrapper = new LambdaQueryWrapper<MetadataTask>()
                .eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                .in(MetadataTask::getDbId, databaseIds);

        List<MetadataTask> list = metadataTaskMapper.selectList(queryWrapper);
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        }

        // 转换为Tree节点
        List<Tree> allNodes = convertToTreeNodes(groups, dbMap, list);

        // 构建树
        List<Tree> tree = treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));

        // 过滤空分组
        return treeBuilder.filterEmptyGroups(tree);
    }

    /**
     * 节点转换
     *
     * @param groups
     * @param metadataTasks
     * @return
     */
    public List<Tree> convertToTreeNodes(List<DbGroup> groups, Map<Integer, DbDatabase> dbMap, List<MetadataTask> metadataTasks) {
        List<Tree> nodes = new ArrayList<>();

        // 转换分组
        for (DbGroup group : groups) {
            Tree tree = new Tree();
            BeanUtils.copyProperties(group, tree);
            tree.setType(Constants.TREE_TYPE.GROUP);
            tree.setOrderBy(group.getOrderBy());
            nodes.add(tree);
        }

        // 转换数据库
        for (MetadataTask task : metadataTasks) {
            DbDatabase database = dbMap.get(task.getDbId());
            Tree tree = new Tree();
            tree.setId(task.getId());
            tree.setName(database.getName());
            tree.setType(Constants.TREE_TYPE.DATABASE);
            tree.setParentId(database.getGroupId());
            tree.setOrderBy(task.getId());
            tree.setIcon(basicService.getDbIcon(database.getType())); // deal icon
            nodes.add(tree);
        }

        return nodes;
    }

    /**
     * 列表
     *
     * @param dbId
     * @param dbName
     * @param groupId
     * @param status
     * @param enable
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public IPage<MetadataTaskVo> list(Integer dbId, String dbName, Integer groupId, String status, Boolean enable, Integer pageNo, Integer pageSize) {

        // 构建查询参数
        Map<String, Object> params = new HashMap<>();
        params.put("dbId", dbId);
        params.put("dbName", StringUtils.isNotBlank(dbName) ? dbName : null);
        params.put("status", status);
        params.put("enable", enable);

        // 分组处理优化
        if (Objects.nonNull(groupId)) {
            List<Integer> dbIds = dbDatabaseService.listDatabasesByGroupId(groupId)
                    .stream().map(DbDatabase::getId).collect(Collectors.toList());

            if (CollectionUtils.isEmpty(dbIds)) {
                // 直接返回空分页对象
                return new Page<>(pageNo, pageSize);
            }
            params.put("dbIds", dbIds);
        }

        // 分页查询
        Page<MetadataTaskVo> page = new Page<>(pageNo, pageSize);
        IPage<MetadataTaskVo> result = metadataTaskMapper.selectTaskList(page, params);

        // 字典翻译优化
        result.getRecords().forEach(this::fillDictText);
        return result;
    }

    /**
     * 详情
     *
     * @param id
     * @return
     */
    @Override
    public MetadataTaskVo detail(Integer id) {
        MetadataTaskVo taskVo = metadataTaskMapper.selectTaskById(id);
        if (taskVo == null) {
            throw new BusinessException(String.format("任务id为[%s]的任务不存在!", id));
        }

        fillDictText(taskVo);
        return taskVo;
    }

    /**
     * 填充字典文本
     *
     * @param vo
     */
    private void fillDictText(MetadataTaskVo vo) {
        vo.setStatusTxt(dictService.getDictItemValue(Constants.DICT.JOB_MANAGE_STATUS, vo.getStatus()));
        vo.setEnableTxt(dictService.getDictItemValue(Constants.DICT.ENABLE,
                vo.getEnable() != null ? vo.getEnable().toString() : null));
        vo.setNotifyPolicyTxt(dictService.getDictItemValue(Constants.DICT.NOTIFY_POLICY, vo.getNotifyPolicy()));
        vo.setFailurePolicyTxt(dictService.getDictItemValue(Constants.DICT.FAILURE_POLICY, vo.getFailurePolicy()));
        vo.setExecuteTimeTxt(DateUtil.formatDateTime(vo.getExecuteTime()));
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));
    }

    /**
     * 新增元数据同步任务
     *
     * @param taskForm
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(MetadataTaskForm taskForm) {
        // 验证数据源是否已存在同步任务
        verifyMetadataTaskIsExists(taskForm.getDbId());

        MetadataTask metadataTask = new MetadataTask();
        BeanUtils.copyProperties(taskForm, metadataTask);
        // 未删除
        metadataTask.setDeleted(Constants.DELETE_FLAG.FALSE);
        metadataTask.setCreatedBy(UserContext.getCurrentAccount());
        metadataTask.setCreatedTime(new Date());
        // 未启动
        metadataTask.setStatus(Constants.JOB_MANAGE_STATUS.INIT);
        // 是否启用
        metadataTask.setEnable(Enable.ENABLE.getCode().equals(taskForm.getEnable()));
        metadataTaskMapper.insert(metadataTask);
    }

    /**
     * 编辑元数据同步任务
     *
     * @param taskForm
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(MetadataTaskUpdateForm taskForm) {
        MetadataTask task = getMetadataTaskById(taskForm.getId());
        BeanUtils.copyProperties(taskForm, task);

        // 未启动
        task.setStatus(Constants.JOB_MANAGE_STATUS.INIT);
        // 编辑后视为重新配置，清空历史失败原因
        task.setReason("");
        // 是否启用
        task.setEnable(Enable.ENABLE.getCode().equals(taskForm.getEnable()));

        task.setUpdatedBy(UserContext.getCurrentAccount());
        task.setUpdatedTime(new Date());
        metadataTaskMapper.updateById(task);
    }

    /**
     * 切换任务启用状态
     *
     * @param id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void toggleEnableStatus(Integer id) {
        MetadataTask task = getMetadataTaskById(id);
        // 是否启用
        task.setEnable(!task.getEnable());

        task.setUpdatedBy(UserContext.getCurrentAccount());
        task.setUpdatedTime(new Date());
        metadataTaskMapper.updateById(task);
    }

    /**
     * 删除同步任务
     *
     * @param id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {
        getMetadataTaskById(id);

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<MetadataTask> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        metadataTaskMapper.update(null, updateWrapper);
    }

    /**
     * 校验数据源是否已存在同步任务
     *
     * @param dbId
     */
    private void verifyMetadataTaskIsExists(Integer dbId) {
        // 获取数据源信息，注：该方法内部已判断数据源是否存在
        DbDatabase database = dbDatabaseService.getDatabaseById(dbId);

        LambdaQueryWrapper<MetadataTask> ldq = new LambdaQueryWrapper<>();
        ldq.eq(MetadataTask::getDbId, dbId)
                .eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE);

        MetadataTask task = metadataTaskMapper.selectOne(ldq);
        if (Objects.nonNull(task)) {
            throw new BusinessException(String.format("请校验数据源【%s】是否已存在元数据同步任务!", database.getName()));
        }
    }

    /**
     * 根据id获取元数据同步任务
     *
     * @param id
     * @return
     */
    private MetadataTask getMetadataTaskById(Integer id) {
        LambdaQueryWrapper<MetadataTask> ldq = new LambdaQueryWrapper<>();
        ldq.eq(MetadataTask::getId, id)
                .eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE);

        MetadataTask task = metadataTaskMapper.selectOne(ldq);
        if (Objects.isNull(task)) {
            throw new BusinessException(String.format("请校验id为【%s】元数据同步任务是否已存在!", id));
        }

        return task;
    }

    /**
     * 立即同步数据
     *
     * @param dbId
     */
    @Override
    public void immediatelySync(Integer dbId) {
        Date now = new Date();
        // 下游消费者会把 scheduledTimeMillis 再截断到秒后与 execute_time 精确匹配
        Date scheduledDate = new Date((now.getTime() / 1000) * 1000);

        LambdaQueryWrapper<MetadataTask> query = new LambdaQueryWrapper<>();
        query.eq(MetadataTask::getDbId, dbId)
                .eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                // 手动触发不依赖 cron（定时调度侧仍然依赖 cron，不需要改）
                .eq(MetadataTask::getEnable, true);

        List<MetadataTask> tasks = metadataTaskMapper.selectList(query);
        if (tasks == null || tasks.isEmpty()) {
            return;
        }

        for (MetadataTask task : tasks) {
            if (task == null) continue;

            // 行为与调度侧保持一致：停止中/已停止直接跳过
            if (task.getStatus() == null) continue;
            if (Constants.JOB_MANAGE_STATUS.STOP.equals(task.getStatus())
                    || Constants.JOB_MANAGE_STATUS.STOPPING.equals(task.getStatus())) {
                continue;
            }

            // 调度侧：failure_policy=1 表示停止，这里保持一致
            if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(task.getStatus())
                    && "1".equals(task.getFailurePolicy())) {
                continue;
            }

            // 手动触发：抢占到 STARTING 再投递，避免重复
            // 但如果正在 RUNNING/STARTING，则只在“卡住”时允许抢占，确保行为与调度侧一致。
            UpdateWrapper<MetadataTask> uw = new UpdateWrapper<>();
            uw.eq("id", task.getId())
                    .eq("deleted", Constants.DELETE_FLAG.FALSE);

            boolean inProgress = Constants.JOB_MANAGE_STATUS.STARTING.equals(task.getStatus())
                    || Constants.JOB_MANAGE_STATUS.RUNNING.equals(task.getStatus());
            if (inProgress) {
                if (task.getExecuteTime() == null) continue;

                long diff = now.getTime() - task.getExecuteTime().getTime();
                if (diff <= stuckThresholdMs) {
                    // 未到卡住阈值，不抢占
                    continue;
                }

                // 已卡住：允许抢占恢复
                Date stuckThresholdDate = new Date(now.getTime() - stuckThresholdMs);
                uw.in("status", Arrays.asList(
                                Constants.JOB_MANAGE_STATUS.STARTING,
                                Constants.JOB_MANAGE_STATUS.RUNNING
                        ))
                        .le("execute_time", stuckThresholdDate)
                        .set("status", Constants.JOB_MANAGE_STATUS.STARTING)
                        .set("execute_time", scheduledDate)
                        .set("reason", "")
                        .set("updated_time", now)
                        .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            } else {
                uw.notIn("status", Arrays.asList(
                                Constants.JOB_MANAGE_STATUS.STARTING,
                                Constants.JOB_MANAGE_STATUS.RUNNING
                        ))
                        .set("status", Constants.JOB_MANAGE_STATUS.STARTING)
                        .set("execute_time", scheduledDate)
                        .set("reason", "")
                        .set("updated_time", now)
                        .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            }

            int updated = metadataTaskMapper.update(null, uw);
            if (updated != 1) continue;

            TaskDispatchMessage msg = new TaskDispatchMessage();
            msg.setTaskType(Constants.TASK_TYPE.METADATA_TASK);
            msg.setTaskId(task.getId());
            msg.setDbId(task.getDbId());
            msg.setScheduledTimeMillis(scheduledDate.getTime());
            taskDispatchKafkaProducer.send(msg);
        }
    }
}
