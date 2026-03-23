package com.pufferfishscheduler.master.collect.realtime.service.impl;

import java.util.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.RtFieldMapper;
import com.pufferfishscheduler.dao.entity.RtSyncLog;
import com.pufferfishscheduler.dao.entity.RtTableMapper;
import com.pufferfishscheduler.dao.entity.RtTableStats;
import com.pufferfishscheduler.dao.entity.RtTask;
import com.pufferfishscheduler.dao.mapper.RtSyncLogMapper;
import com.pufferfishscheduler.dao.mapper.RtTableStatsMapper;
import com.pufferfishscheduler.dao.mapper.RtTaskMapper;
import com.pufferfishscheduler.domain.form.collect.RealTimeFieldMapperForm;
import com.pufferfishscheduler.domain.form.collect.RealTimeTableForm;
import com.pufferfishscheduler.domain.form.collect.RealTimeTaskForm;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.model.database.DatabaseField;
import com.pufferfishscheduler.domain.vo.collect.RealTimeFieldMapperVo;
import com.pufferfishscheduler.domain.vo.collect.RealTimeTableMapperVo;
import com.pufferfishscheduler.domain.vo.collect.RealTimeTaskVo;
import com.pufferfishscheduler.cdc.kafka.RealTimeDataSyncEngine;
import com.pufferfishscheduler.cdc.kafka.RealtimeStatsRedisReader;
import com.pufferfishscheduler.cdc.kafka.RealtimeStatsRedisWriter;
import com.pufferfishscheduler.cdc.kafka.config.KafkaDataProperties;
import com.pufferfishscheduler.cdc.kafka.database.DataSourceAdapter;
import com.pufferfishscheduler.cdc.kafka.database.DataSourceAdapterFactory;
import com.pufferfishscheduler.cdc.kafka.entity.*;
import com.pufferfishscheduler.master.collect.realtime.service.RealTimeTaskService;
import com.pufferfishscheduler.master.collect.realtime.service.RtFieldMapperService;
import com.pufferfishscheduler.master.collect.realtime.service.RtTableMapperService;
import com.pufferfishscheduler.master.database.database.service.DbFieldService;
import com.pufferfishscheduler.master.common.dict.service.DictService;

import java.util.stream.Collectors;
import java.util.concurrent.*;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;

import lombok.extern.slf4j.Slf4j;

/**
 * 实时数据同步任务服务实现类
 *
 * @author Mayc
 * @since 2026-03-13 17:44
 */
@Slf4j
@Service
public class RealTimeTaskServiceImpl implements RealTimeTaskService {
    private static final long STOP_TIMEOUT_SECONDS = 30L;

    /**
     * 按任务 ID 串行化 start()，避免定时任务 10s 一轮与 start() 内 60s+ 轮询叠加导致同一任务并发启动、状态落库被覆盖或丢失。
     */
    private static final ConcurrentHashMap<Integer, Object> RT_START_LOCKS = new ConcurrentHashMap<>();

    private static Object rtStartLock(Integer taskId) {
        return RT_START_LOCKS.computeIfAbsent(taskId, k -> new Object());
    }

    /** 启动/调度失败落库最大重试次数（含首次） */
    private static final int FAILURE_PERSIST_MAX_ATTEMPTS = 5;
    /** 重试间隔（毫秒） */
    private static final long FAILURE_PERSIST_RETRY_INTERVAL_MS = 200L;

    @Autowired
    private DictService dictService;

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private DbFieldService dbFieldService;

    @Autowired
    private RtTaskMapper rtTaskMapper;

    @Autowired
    private RtTableMapperService rtTableMapperService;

    @Autowired
    private RtFieldMapperService rtFieldMapperService;

    @Autowired
    private KafkaDataProperties kafkaDataProperties;

    @Autowired
    private RealtimeStatsRedisReader realtimeStatsRedisReader;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RtTableStatsMapper rtTableStatsMapper;

    @Autowired
    private RtSyncLogMapper rtSyncLogMapper;

    /**
     * 自注入：确保 {@link #updateTaskStatus} 的 REQUIRES_NEW 在同类调用中生效（避免 this 自调用导致事务切面失效）。
     */
    @Lazy
    @Autowired
    private RealTimeTaskService self;

    @Value("${realtime.config.heartbeat.enabled}")
    public Boolean heartbeatEnabled;

    @Value("${realtime.config.heartbeat.interval.ms}")
    public Integer heartbeatIntervalMs;

    /**
     * 查询实时数据同步任务列表
     *
     * @param taskName   任务名称
     * @param sourceDbId 源数据库ID
     * @param targetDbId 目标数据库ID
     * @param taskStatus 任务状态
     * @param pageNo     页码
     * @param pageSize   每页数量
     * @return 实时数据同步任务VO列表
     */
    @Override
    public IPage<RealTimeTaskVo> list(String taskName, Integer sourceDbId, Integer targetDbId, String taskStatus,
                                      Integer pageNo, Integer pageSize) {

        // 1.构建分页对象（确保页码/页大小合法）
        Page<RtTask> pageParam = new Page<>(pageNo, pageSize);

        // 2.建查询条件并执行分页查询
        LambdaQueryWrapper<RtTask> queryWrapper = buildQueryWrapper(taskName, sourceDbId, targetDbId, taskStatus);
        Page<RtTask> page = rtTaskMapper.selectPage(pageParam, queryWrapper);

        // 3.空结果处理（关键优化：直接拷贝所有分页属性）
        Page<RealTimeTaskVo> result = new Page<>();
        BeanUtils.copyProperties(page, result);

        // 4.获取源数据库和目标数据库名称
        List<DbDatabase> dbDatabases = dbDatabaseService.listByCategory(Constants.Category.R);
        Map<Integer, String> databaseMap = dbDatabases.stream()
                .collect(Collectors.toMap(db -> db.getId(), db -> db.getName()));

        // 5.转换VO
        if (CollectionUtils.isEmpty(page.getRecords())) {
            result.setRecords(Collections.emptyList());
        } else {
            result.setRecords(page.getRecords().stream()
                    .map(task -> convertToVo(task, databaseMap))
                    .collect(Collectors.toList()));
        }
        return result;
    }

    /**
     * 根据任务状态查询任务列表
     *
     * @param taskStatus 任务状态列表
     * @return 数据同步任务实体列表
     */
    @Override
    public List<RtTask> getTaskListByStatus(List<String> taskStatus) {
        if (CollectionUtils.isEmpty(taskStatus)) {
            return List.of();
        }

        LambdaQueryWrapper<RtTask> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                .in(RtTask::getStatus, taskStatus);

        return rtTaskMapper.selectList(queryWrapper);
    }

    /**
     * 构建查询条件
     *
     * @param taskName
     * @param sourceDbId
     * @param targetDbId
     * @param taskStatus
     * @return
     */
    private LambdaQueryWrapper<RtTask> buildQueryWrapper(String taskName, Integer sourceDbId, Integer targetDbId,
                                                         String taskStatus) {
        LambdaQueryWrapper<RtTask> queryWrapper = new LambdaQueryWrapper<>();
        if (StringUtils.isNotBlank(taskName)) {
            queryWrapper.like(RtTask::getName, taskName);
        }
        if (Objects.nonNull(sourceDbId)) {
            queryWrapper.eq(RtTask::getSourceDbId, sourceDbId);
        }

        if (Objects.nonNull(targetDbId)) {
            queryWrapper.eq(RtTask::getTargetDbId, targetDbId);
        }
        if (Objects.nonNull(taskStatus)) {
            queryWrapper.eq(RtTask::getStatus, taskStatus);
        }
        queryWrapper.eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(RtTask::getCreatedTime);
        return queryWrapper;
    }

    /**
     * 转换为VO
     *
     * @param rtTask
     * @param databaseMap
     * @return
     */
    private RealTimeTaskVo convertToVo(RtTask rtTask, Map<Integer, String> databaseMap) {
        RealTimeTaskVo vo = new RealTimeTaskVo();
        vo.setTaskId(rtTask.getId());
        vo.setTaskName(rtTask.getName());
        vo.setTaskStatus(rtTask.getStatus());
        vo.setSourceDbId(rtTask.getSourceDbId());
        vo.setSourceDbName(databaseMap.getOrDefault(rtTask.getSourceDbId(), ""));
        vo.setTargetDbId(rtTask.getTargetDbId());
        vo.setTargetDbName(databaseMap.getOrDefault(rtTask.getTargetDbId(), ""));
        vo.setEngineType(rtTask.getEngineType());
        vo.setEngineTypeTxt(dictService.getDictItemValue(Constants.DICT.CDC_ENGINE_TYPE, vo.getEngineType()));
        vo.setTaskStatusTxt(dictService.getDictItemValue(Constants.DICT.JOB_MANAGE_STATUS, vo.getTaskStatus()));
        vo.setReason(rtTask.getReason());
        vo.setCreatedTime(rtTask.getCreatedTime());
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));
        return vo;
    }

    /**
     * 添加实时数据同步任务
     *
     * @param taskForm 实时数据同步任务表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void add(RealTimeTaskForm taskForm) {
        validateRealTimeTaskForm(taskForm);
        verifyTaskExist(taskForm.getSourceDbId(), taskForm.getTargetDbId());

        String account = UserContext.getCurrentAccount();
        Date now = new Date();

        RtTask task = buildAndInsertTask(taskForm, account, now);
        if (CollectionUtils.isEmpty(taskForm.getTableMappers())) {
            return;
        }

        for (RealTimeTableForm tableForm : taskForm.getTableMappers()) {
            RtTableMapper tableMapperEntity = buildAndInsertTableMapper(task.getId(), tableForm, account, now);
            insertFieldMappers(task.getId(), tableMapperEntity.getId(), tableForm.getFieldMappers(), account, now);
        }
    }

    /**
     * 构建并插入实时数据同步任务
     *
     * @param taskForm 实时数据同步任务表单
     * @param account  创建人账号
     * @param now      创建时间
     * @return 实时数据同步任务实体
     */
    private RtTask buildAndInsertTask(RealTimeTaskForm taskForm, String account, Date now) {
        RtTask task = new RtTask();
        task.setName(taskForm.getTaskName());
        task.setSourceDbId(taskForm.getSourceDbId());
        task.setTargetDbId(taskForm.getTargetDbId());
        task.setEngineType(taskForm.getEngineType());
        task.setSyncType(taskForm.getSyncType());
        task.setHeartbeatEnabled(heartbeatEnabled);
        task.setHeartbeatInterval(heartbeatIntervalMs);
        task.setStatus(Constants.JOB_MANAGE_STATUS.INIT);
        task.setDeleted(Constants.DELETE_FLAG.FALSE);
        task.setCreatedBy(account);
        task.setCreatedTime(now);
        rtTaskMapper.insert(task);
        return task;
    }

    /**
     * 构建并插入实时数据同步任务表映射
     *
     * @param taskId    实时数据同步任务ID
     * @param tableForm 实时数据同步任务表映射表单
     * @param account   创建人账号
     * @param now       创建时间
     * @return 实时数据同步任务表映射实体
     */
    private RtTableMapper buildAndInsertTableMapper(Integer taskId, RealTimeTableForm tableForm, String account,
                                                    Date now) {
        RtTableMapper entity = new RtTableMapper();
        entity.setTaskId(taskId);
        entity.setSourceTableId(tableForm.getSourceTableId());
        entity.setSourceTableName(tableForm.getSourceTableName());
        entity.setTargetTableId(tableForm.getTargetTableId());
        entity.setTargetTableName(tableForm.getTargetTableName());
        entity.setDeleteDataFlag(tableForm.getDeleteFlag());
        entity.setParallelWriteFlag(tableForm.getParallelWriteFlag());
        entity.setParallelThreadNum(tableForm.getParallelThreadNum());
        entity.setWriteType(tableForm.getWriteType());
        entity.setBatchSize(tableForm.getBatchSize());
        entity.setDeleted(Constants.DELETE_FLAG.FALSE);
        entity.setCreatedBy(account);
        entity.setCreatedTime(now);
        rtTableMapperService.saveWithoutTransaction(entity);
        return entity;
    }

    /**
     * 构建并插入实时数据同步任务字段映射
     *
     * @param taskId        实时数据同步任务ID
     * @param tableMapperId 实时数据同步任务表映射ID
     * @param fieldMappers  实时数据同步任务字段映射表单列表
     * @param account       创建人账号
     * @param now           创建时间
     */
    private void insertFieldMappers(Integer taskId, Integer tableMapperId, List<RealTimeFieldMapperForm> fieldMappers,
                                    String account, Date now) {
        if (CollectionUtils.isEmpty(fieldMappers)) {
            return;
        }

        // 构建实时数据同步任务字段映射实体列表
        List<RtFieldMapper> rtFieldMappers = new LinkedList<>();
        for (RealTimeFieldMapperForm form : fieldMappers) {
            RtFieldMapper entity = new RtFieldMapper();
            entity.setTaskId(taskId);
            entity.setTableMapperId(tableMapperId);
            entity.setSourceFieldId(form.getSourceFieldId());
            entity.setTargetFieldId(form.getTargetFieldId());
            entity.setSourceFieldName(form.getSourceFieldName());
            entity.setTargetFiledName(form.getTargetFieldName());
            entity.setDeleted(Constants.DELETE_FLAG.FALSE);
            entity.setCreatedBy(account);
            entity.setCreatedTime(now);
            rtFieldMappers.add(entity);
        }

        rtFieldMapperService.batchSaveWithoutTransaction(rtFieldMappers);
    }

    /**
     * 更新实时数据同步任务
     * 编辑前校验：任务存在；来源库+目标库组合未被其他任务占用；任务状态为未启动/已停止/失败状态方可修改。
     *
     * @param taskForm 实时数据同步任务表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void update(RealTimeTaskForm taskForm) {
        validateRealTimeTaskForm(taskForm);
        if (taskForm.getTaskId() == null) {
            throw new BusinessException("任务ID不能为空!");
        }

        RtTask task = getRtTaskByTaskId(taskForm.getTaskId());
        ensureTaskStatusEditable(task.getStatus());
        verifyTaskExistForUpdate(taskForm.getSourceDbId(), taskForm.getTargetDbId(), taskForm.getTaskId());

        String account = UserContext.getCurrentAccount();
        Date now = new Date();

        task.setName(taskForm.getTaskName());
        task.setSourceDbId(taskForm.getSourceDbId());
        task.setTargetDbId(taskForm.getTargetDbId());
        task.setEngineType(taskForm.getEngineType());
        task.setSyncType(taskForm.getSyncType());
        task.setUpdatedBy(account);
        task.setUpdatedTime(now);
        rtTaskMapper.updateById(task);

        // 删除关联的字段映射
        rtFieldMapperService.deleteByTaskId(task.getId());

        // 删除关联的表映射
        rtTableMapperService.deleteByTaskId(task.getId());

        if (CollectionUtils.isEmpty(taskForm.getTableMappers())) {
            return;
        }
        for (RealTimeTableForm tableForm : taskForm.getTableMappers()) {
            RtTableMapper tableMapperEntity = buildAndInsertTableMapper(task.getId(), tableForm, account, now);
            insertFieldMappers(task.getId(), tableMapperEntity.getId(), tableForm.getFieldMappers(), account, now);
        }
    }

    /**
     * 仅允许 未启动、已停止、失败状态的任务被修改
     */
    private void ensureTaskStatusEditable(String status) {
        boolean editable = Constants.JOB_MANAGE_STATUS.INIT.equals(status)
                || Constants.JOB_MANAGE_STATUS.STOP.equals(status)
                || Constants.JOB_MANAGE_STATUS.FAILURE.equals(status);
        if (!editable) {
            throw new BusinessException("仅未启动、已停止、或失败状态的任务允许修改!");
        }
    }

    /**
     * 校验来源库+目标库是否已被其他任务占用（排除指定任务ID，用于编辑场景）
     */
    private void verifyTaskExistForUpdate(Integer sourceDbId, Integer targetDbId, Integer excludeTaskId) {
        QueryWrapper<RtTask> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("source_db_id", sourceDbId)
                .eq("target_db_id", targetDbId)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .ne("id", excludeTaskId);
        if (rtTaskMapper.selectCount(queryWrapper) > 0) {
            throw new BusinessException("当前来源库与目标库组合已被其他实时任务占用!");
        }
    }

    /**
     * 删除实时数据同步任务（逻辑删除）
     * 先校验任务是否存在，再校验任务状态：仅未启动、已停止、失败状态可删除。
     *
     * @param taskId 实时数据同步任务ID
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(Integer taskId) {
        if (taskId == null) {
            throw new BusinessException("任务ID不能为空!");
        }
        RtTask task = getRtTaskByTaskId(taskId);
        ensureTaskStatusEditable(task.getStatus());

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<RtTask> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", taskId)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());
        rtTaskMapper.update(null, updateWrapper);

        // 删除关联的字段映射
        rtFieldMapperService.deleteByTaskId(task.getId());

        // 删除关联的表映射
        rtTableMapperService.deleteByTaskId(task.getId());
    }

    /**
     * 查询实时数据同步任务详情（含表映射、字段映射）
     *
     * @param taskId 实时数据同步任务ID
     * @return 实时数据同步任务VO
     */
    @Override
    public RealTimeTaskVo detail(Integer taskId) {
        RtTask task = getRtTaskByTaskId(taskId);
        Map<Integer, String> databaseMap = dbDatabaseService.listByCategory(Constants.Category.R).stream()
                .collect(Collectors.toMap(db -> db.getId(), db -> db.getName()));
        RealTimeTaskVo vo = convertToVo(task, databaseMap);

        // 查询表映射（未删除）
        LambdaQueryWrapper<RtTableMapper> tableWrapper = new LambdaQueryWrapper<>();
        tableWrapper.eq(RtTableMapper::getTaskId, taskId)
                .eq(RtTableMapper::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByAsc(RtTableMapper::getId);
        List<RtTableMapper> tableMappers = rtTableMapperService.list(tableWrapper);

        List<RealTimeTableMapperVo> tableMapperVoList = new ArrayList<>();
        for (RtTableMapper tm : tableMappers) {
            RealTimeTableMapperVo tmVo = new RealTimeTableMapperVo();
            tmVo.setId(tm.getId());
            tmVo.setSourceTableId(tm.getSourceTableId());
            tmVo.setSourceTableName(tm.getSourceTableName());
            tmVo.setTargetTableId(tm.getTargetTableId());
            tmVo.setTargetTableName(tm.getTargetTableName());
            tmVo.setDeleteDataFlag(tm.getDeleteDataFlag());
            tmVo.setParallelWriteFlag(tm.getParallelWriteFlag());
            tmVo.setParallelThreadNum(tm.getParallelThreadNum());
            tmVo.setWriteType(tm.getWriteType());
            tmVo.setBatchSize(tm.getBatchSize());

            // 查询该表映射下的字段映射（未删除）
            LambdaQueryWrapper<RtFieldMapper> fieldWrapper = new LambdaQueryWrapper<>();
            fieldWrapper.eq(RtFieldMapper::getTableMapperId, tm.getId())
                    .eq(RtFieldMapper::getDeleted, Constants.DELETE_FLAG.FALSE)
                    .orderByAsc(RtFieldMapper::getId);
            List<RtFieldMapper> fieldMappers = rtFieldMapperService.list(fieldWrapper);
            List<RealTimeFieldMapperVo> fieldVoList = fieldMappers.stream()
                    .map(this::convertToFieldMapperVo)
                    .collect(Collectors.toList());
            tmVo.setFieldMappers(fieldVoList);
            tableMapperVoList.add(tmVo);
        }
        vo.setTableMappers(tableMapperVoList);
        return vo;
    }

    /**
     * 立即启动实时数据同步任务（仅未启动、已停止、失败状态可启动）
     *
     * @param taskId 实时数据同步任务ID
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void immediatelyStart(Integer taskId) {
        // 校验任务是否存在
        RtTask task = getRtTaskByTaskId(taskId);
        // 校验任务状态是否可启动
        ensureTaskStatusEditable(task.getStatus());
        // 仅更新为“启动中”，reason 由真正执行 start/stop 的结果来写入
        self.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.STARTING, "", UserContext.getCurrentAccount(), "");
    }

    /**
     * 立即停止实时数据同步任务（仅运行中可停止）
     *
     * @param taskId 实时数据同步任务ID
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void immediatelyStop(Integer taskId) {
        // 校验任务是否存在
        RtTask task = getRtTaskByTaskId(taskId);
        // 放宽校验：只要不是“已停止”或“初始”状态，都应该允许用户点击停止
        List<String> canStopStatus = List.of(
                Constants.JOB_MANAGE_STATUS.RUNNING,
                Constants.JOB_MANAGE_STATUS.FAILURE,
                Constants.JOB_MANAGE_STATUS.STARTING // 允许停止正在启动中的任务
        );
        if (!canStopStatus.contains(task.getStatus())) {
            throw new BusinessException("当前任务状态无需停止!");
        }
        // 仅更新为“停止中”，reason 由真正执行 stop 的结果来写入
        self.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.STOPPING, "", UserContext.getCurrentAccount(), "");
    }

    /**
     * 启动实时数据同步任务
     *
     * @param taskId 任务ID
     */
    @Override
    public void start(Integer taskId) {
        if (taskId == null) {
            return;
        }
        synchronized (rtStartLock(taskId)) {
            RtTask rtTask = getRtTaskByTaskId(taskId);
            // 仅处理「启动中」：调度器如此调用；持锁后再次读库，若上一轮已落库 FAILURE 则直接退出，避免并发 start 把状态冲掉
            if (!Constants.JOB_MANAGE_STATUS.STARTING.equals(rtTask.getStatus())) {
                log.debug("实时任务 start 跳过：库表状态非 STARTING，taskId={}, status={}", taskId, rtTask.getStatus());
                return;
            }

            try {
                // 长流程（Kafka Connect / 线程等）不应占用 Spring 事务连接
                RealTimeDataSyncEngine syncEngine = new RealTimeDataSyncEngine(kafkaDataProperties.getBrokers(), kafkaDataProperties.getConnector());
                DataSyncTask task = buildDataSyncTask(rtTask);

                // 这里的 syncEngine.start 现在是阻塞的，直到真的运行成功或明确失败
                RuntimeConfig runtimeConfig = syncEngine.start(task);
                rtTask.setStatus(Constants.JOB_MANAGE_STATUS.RUNNING);
                rtTask.setRuntimeConfig(JSONObject.toJSONString(runtimeConfig));
            } catch (Exception e) {
                log.error("", e);
                rtTask.setStatus(Constants.JOB_MANAGE_STATUS.FAILURE);
                String msg = e.getMessage();
                if (StringUtils.isBlank(msg)) {
                    msg = e.toString();
                }
                rtTask.setReason("任务启动失败！原因：" + msg);
            } finally {
                try {
                    rtTask.setUpdatedBy(Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
                    rtTask.setUpdatedTime(new Date());
                    if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(rtTask.getStatus())) {
                        // 先 XML 按主键更新（不校验 deleted），避免 MyBatis-Plus 条件更新 0 行导致长期卡在 STARTING
                        try {
                            int xmlRows = rtTaskMapper.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.FAILURE,
                                    StringUtils.defaultString(rtTask.getReason()));
                            log.info("实时任务启动失败，已 XML 直写 FAILURE，taskId={}, rows={}", taskId, xmlRows);
                        } catch (Exception xmlEx) {
                            log.error("实时任务启动失败 XML 直写异常，taskId={}", taskId, xmlEx);
                        }
                        persistFailureWithRetry(taskId, rtTask.getReason(), Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                                rtTask.getRuntimeConfig());
                    } else {
                        int rows = self.updateTaskStatus(taskId, rtTask.getStatus(),
                                StringUtils.defaultString(rtTask.getReason()),
                                Constants.SYS_OP_INFO.SYSTEM_ACCOUNT, rtTask.getRuntimeConfig());
                        if (rows == 0) {
                            log.error("实时任务启动成功但状态落库影响行数为0，taskId={}, status={}", taskId, rtTask.getStatus());
                        }
                    }
                } catch (Exception finallyEx) {
                    log.error("实时任务 start() finally 落库异常，taskId={}", taskId, finallyEx);
                    if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(rtTask.getStatus())) {
                        try {
                            rtTaskMapper.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.FAILURE,
                                    StringUtils.defaultString(rtTask.getReason()));
                        } catch (Exception ignore) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    /**
     * 停止实时数据同步任务
     *
     * @param taskId 任务ID
     */
    @Override
    public void stop(Integer taskId) {
        RtTask rtTask = getRtTaskByTaskId(taskId);

        try {
            RealTimeDataSyncEngine syncEngine = new RealTimeDataSyncEngine(kafkaDataProperties.getBrokers(), kafkaDataProperties.getConnector());
            DataSyncTask task = buildDataSyncTask(rtTask);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> future = executor.submit(() -> syncEngine.stop(task));
            try {
                future.get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                future.cancel(true);
                executor.shutdownNow();
            }
            rtTask.setStatus(Constants.JOB_MANAGE_STATUS.STOP);
            rtTask.setReason("");
            rtTask.setRuntimeConfig("");
            log.info("停止实时任务成功，taskId={}", taskId);
        } catch (TimeoutException e) {
            log.error("停止实时任务超时，taskId={}", taskId, e);
            rtTask.setStatus(Constants.JOB_MANAGE_STATUS.FAILURE);
            rtTask.setReason("任务停止失败！原因：停止超时（超过 " + STOP_TIMEOUT_SECONDS + " 秒）");
        } catch (Exception e) {
            log.error("", e);
            rtTask.setStatus(Constants.JOB_MANAGE_STATUS.FAILURE);
            String msg = e.getMessage();
            if (StringUtils.isBlank(msg)) {
                msg = e.toString();
            }
            rtTask.setReason("任务停止失败！原因：" + msg);
        } finally {
            // stop 可能由定时任务线程触发，此时 UserContext 为空；统一使用 SYSTEM 账户确保可落库
            if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(rtTask.getStatus())) {
                persistFailureWithRetry(taskId, rtTask.getReason(), Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                        rtTask.getRuntimeConfig());
            } else {
                self.updateTaskStatus(taskId, rtTask.getStatus(), StringUtils.defaultString(rtTask.getReason()),
                        Constants.SYS_OP_INFO.SYSTEM_ACCOUNT, rtTask.getRuntimeConfig());
            }
            log.info("停止任务状态已落库，taskId={}, status={}", taskId, rtTask.getStatus());
        }
    }

    /**
     * 定时任务调度失败时标记任务失败，仅供 RealTimeSyncTaskJob 使用
     */
    @Override
    public void markFailedFromScheduler(Integer taskId, String reason) {
        String msg = StringUtils.defaultIfBlank(reason, "定时任务调度失败");
        persistFailureWithRetry(taskId, msg, Constants.SYS_OP_INFO.SYSTEM_ACCOUNT, null);
    }

    /**
     * 构建实时数据同步任务实体
     *
     * @param rtTask 实时数据同步任务
     * @return 实时数据同步任务实体
     */
    private DataSyncTask buildDataSyncTask(RtTask rtTask) {
        DataSyncTask syncTask = new DataSyncTask();
        syncTask.setTaskId(rtTask.getId());
        syncTask.setDataSyncType(rtTask.getSyncType());

        // TODO 后续区分是Kafka还是Flink
//        String engineType = rtTask.getEngineType();
        syncTask.setRuntimeConfig(rtTask.getRuntimeConfig());
        syncTask.setHeartbeatEnabled(rtTask.getHeartbeatEnabled());
        syncTask.setHeartbeatInterval(rtTask.getHeartbeatInterval());
        syncTask.setHeartbeatInterval(rtTask.getHeartbeatInterval());
        syncTask.setBrokers(kafkaDataProperties.getBrokers());

        // 数据库配置
        DbDatabase sourceDatabase = dbDatabaseService.getDatabaseById(rtTask.getSourceDbId());
        DbDatabase targetDatabase = dbDatabaseService.getDatabaseById(rtTask.getTargetDbId());

        // 数据源适配器-来源
        syncTask.setSourceDatabase(buildDatabaseAdapter(sourceDatabase));
        // 数据源适配器-目标
        syncTask.setTargetDatabase(buildDatabaseAdapter(targetDatabase));

        // 表映射
        List<RtTableMapper> rtTableMapperList = rtTableMapperService.selectByTaskId(rtTask.getId());
        List<TableMapper> tableMapperList = new ArrayList<>();
        for (RtTableMapper rtTableMapper : rtTableMapperList) {
            TableMapper tableMapper = new TableMapper(rtTableMapper);
            tableMapper.setTableMapperId(rtTableMapper.getId());

            // 字段
            List<RtFieldMapper> rtFieldMapperList = rtFieldMapperService.selectByTableMapperId(rtTableMapper.getId());
            List<FieldMapper> fieldMapperList = new ArrayList<>();
            //判断映射中是否已经包含全部主键
            Map<String, RtFieldMapper> primaryKeyMap = checkPrimaryKey(rtFieldMapperList, rtTableMapper.getSourceTableId());
            for (RtFieldMapper rtFieldMapper : rtFieldMapperList) {
                RtFieldMapper fieldMapper = primaryKeyMap.get(rtFieldMapper.getSourceFieldName());
                FieldMapper field = null;
                if (null == fieldMapper) {
                    field = new FieldMapper(rtFieldMapper.getSourceFieldName(), rtFieldMapper.getTargetFiledName(), false);
                } else {
                    field = new FieldMapper(rtFieldMapper.getSourceFieldName(), rtFieldMapper.getTargetFiledName(), true);
                }
                fieldMapperList.add(field);
            }
            tableMapper.setFieldMappers(fieldMapperList);
            tableMapperList.add(tableMapper);
        }

        syncTask.setTableMappers(tableMapperList);
        return syncTask;
    }

    /**
     * 构建数据源适配器
     *
     * @param database 数据库配置
     * @return 数据源适配器
     */
    private DataSourceAdapter buildDatabaseAdapter(DbDatabase database) {
        DataSourceAdapter adapter = DataSourceAdapterFactory.getDataSourceAdapter(database.getType());
        DataSourceAdapter.DataBaseType type = DataSourceAdapter.DataBaseType.valueOf(database.getType());
        adapter.setType(type);
        DatabaseConnectionInfo connectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(database, connectionInfo);
        adapter.setConnectionInfo(connectionInfo);
        return adapter;
    }

    /**
     * 根据映射的字段判断主键是否都在映射字段中
     *
     * @param rtFieldMapperList
     * @param tableId
     * @return
     */
    public Map<String, RtFieldMapper> checkPrimaryKey(List<RtFieldMapper> rtFieldMapperList, Integer tableId) {
        List<DatabaseField> fieldList = dbFieldService.getFieldsByTableId(tableId);
        Map<String, RtFieldMapper> mapperMap = new HashMap<>();
        List<String> primaryKeyNameList = new ArrayList<>();
        for (DatabaseField databaseField : fieldList) {
            if (databaseField.getPrimaryKey()) {
                primaryKeyNameList.add(databaseField.getName());
            }
        }
        for (RtFieldMapper rtFieldMapper : rtFieldMapperList) {
            if (primaryKeyNameList.contains(rtFieldMapper.getSourceFieldName())) {
                mapperMap.put(rtFieldMapper.getSourceFieldName(), rtFieldMapper);
                primaryKeyNameList.remove(rtFieldMapper.getSourceFieldName());
            }
        }
        // 如果primaryKeyNameList为0则代表主键都存在映射字段中或者该表没有任何主键
        if (primaryKeyNameList.size() <= 0) {
            return mapperMap;
        }
        // 如果primaryKeyNameList不为0则代表映射中不包含所有主键返回空map
        return new HashMap<>();
    }

    /**
     * 更新实时数据同步任务状态
     *
     * @param taskId        实时数据同步任务ID
     * @param newStatus     新状态
     * @param reason        状态变更原因
     * @param operator      操作人
     * @param runtimeConfig 运行时配置
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public int updateTaskStatus(Integer taskId, String newStatus, String reason,
                                String operator, String runtimeConfig) {
        if (taskId == null || StringUtils.isEmpty(newStatus)) {
            return 0;
        }

        RtTask rtTask = new RtTask();
        rtTask.setId(taskId);
        rtTask.setStatus(newStatus);
        rtTask.setReason(StringUtils.isEmpty(reason) ? "" : reason);
        rtTask.setUpdatedTime(new Date());

        // 操作人
        if (StringUtils.isNotEmpty(operator)) {
            rtTask.setUpdatedBy(operator);
        } else {
            rtTask.setUpdatedBy(UserContext.getCurrentAccount());
        }

        // 运行时配置
        if (StringUtils.isNotEmpty(runtimeConfig)) {
            rtTask.setRuntimeConfig(runtimeConfig);
        }

        // 执行更新，带软删除过滤
        UpdateWrapper<RtTask> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", taskId)
                .eq("deleted", Constants.DELETE_FLAG.FALSE);

        return rtTaskMapper.update(rtTask, updateWrapper);
    }

    /**
     * 失败状态强制落库：通过代理调用 {@link #updateTaskStatus}（REQUIRES_NEW）并多次重试；仍失败则用 Mapper XML 按主键兜底更新（不校验 deleted）。
     */
    private void persistFailureWithRetry(Integer taskId, String reason, String operator, String runtimeConfig) {
        String op = StringUtils.defaultIfBlank(operator, Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        String r = StringUtils.defaultString(reason);
        for (int attempt = 1; attempt <= FAILURE_PERSIST_MAX_ATTEMPTS; attempt++) {
            try {
                int rows = self.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.FAILURE, r, op, runtimeConfig);
                if (rows > 0) {
                    log.info("实时任务失败状态已落库，taskId={}, attempt={}", taskId, attempt);
                    return;
                }
                log.warn("实时任务失败状态落库影响行数为0，将重试，taskId={}, attempt={}", taskId, attempt);
            } catch (Exception e) {
                log.error("实时任务失败状态落库异常，将重试，taskId={}, attempt={}", taskId, attempt, e);
            }
            sleepQuietlyBeforeRetry(attempt);
        }
        fallbackXmlFailureUpdate(taskId, r);
    }

    private void sleepQuietlyBeforeRetry(int attemptSoFar) {
        if (attemptSoFar >= FAILURE_PERSIST_MAX_ATTEMPTS) {
            return;
        }
        try {
            Thread.sleep(FAILURE_PERSIST_RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void fallbackXmlFailureUpdate(Integer taskId, String reason) {
        try {
            int rows = rtTaskMapper.updateTaskStatus(taskId, Constants.JOB_MANAGE_STATUS.FAILURE, reason);
            if (rows > 0) {
                log.warn("实时任务失败状态已通过 XML 兜底（按 id）落库，taskId={}", taskId);
            } else {
                log.error("实时任务失败状态兜底仍失败（无匹配行），taskId={}", taskId);
            }
        } catch (Exception e) {
            log.error("实时任务失败状态兜底仍异常，taskId={}", taskId, e);
        }
    }

    /**
     * 获取实时数据同步任务累计统计
     */
    @Override
    public Map<String, Object> getCumulativeStats(Integer taskId, Integer tableMapperId) {
        Map<String, Object> fromRedis = realtimeStatsRedisReader.getCumulativeStats(taskId, tableMapperId);
        if (fromRedis != null && !fromRedis.isEmpty()) {
            return fromRedis;
        }
        return loadCumulativeStatsFromDb(taskId, tableMapperId);
    }

    /**
     * 获取实时数据同步任务按小时统计
     */
    @Override
    public Map<String, Long> getHourlyStats(Integer taskId, Integer tableMapperId, int syncDate, int syncHour) {
        if (taskId == null || tableMapperId == null) {
            return emptyHourlyStatsMap();
        }
        String key = RealtimeStatsRedisWriter.buildLogKey(taskId, tableMapperId, syncDate, syncHour);
        try {
            if (stringRedisTemplate.hasKey(key)) {
                return realtimeStatsRedisReader.getHourlyStats(taskId, tableMapperId, syncDate, syncHour);
            }
        } catch (Exception e) {
            log.warn("按小时统计读 Redis 失败，将尝试读库, taskId={}, tableMapperId={}", taskId, tableMapperId, e);
        }
        return loadHourlyStatsFromDb(taskId, tableMapperId, syncDate, syncHour);
    }

    /**
     * 与 {@link com.pufferfishscheduler.cdc.kafka.RealtimeStatsRedisSyncJob} 写入的 rt_table_stats 对齐。
     */
    private Map<String, Object> loadCumulativeStatsFromDb(Integer taskId, Integer tableMapperId) {
        Map<String, Object> out = new HashMap<>();
        if (taskId == null || tableMapperId == null) {
            return out;
        }
        try {
            RtTableStats stats = rtTableStatsMapper.selectByTableMapperId(tableMapperId);
            if (stats == null) {
                return out;
            }
            if (stats.getTaskId() != null && !stats.getTaskId().equals(taskId)) {
                log.warn("rt_table_stats.task_id 与请求 taskId 不一致，忽略库表记录, taskId={}, dbTaskId={}, tableMapperId={}",
                        taskId, stats.getTaskId(), tableMapperId);
                return out;
            }
            out.put("last_idv", defaultLong(stats.getLastIdv()));
            out.put("last_udv", defaultLong(stats.getLastUdv()));
            out.put("last_ddv", defaultLong(stats.getLastDdv()));
            out.put("today_idv", defaultLong(stats.getTodayIdv()));
            out.put("today_udv", defaultLong(stats.getTodayUdv()));
            out.put("today_ddv", defaultLong(stats.getTodayDdv()));
            if (stats.getUpdatedTime() != null) {
                out.put("updated_at", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                        LocalDateTime.ofInstant(stats.getUpdatedTime().toInstant(), ZoneId.systemDefault())));
            }
        } catch (Exception e) {
            log.warn("从 rt_table_stats 读取累计统计失败, taskId={}, tableMapperId={}", taskId, tableMapperId, e);
        }
        return out;
    }

    /**
     * 与 {@link com.pufferfishscheduler.cdc.kafka.RealtimeStatsRedisSyncJob} 写入的 rt_sync_log 对齐。
     */
    private Map<String, Long> loadHourlyStatsFromDb(Integer taskId, Integer tableMapperId, int syncDate, int syncHour) {
        Map<String, Long> out = emptyHourlyStatsMap();
        try {
            LambdaQueryWrapper<RtSyncLog> q = new LambdaQueryWrapper<>();
            q.eq(RtSyncLog::getTaskId, taskId)
                    .eq(RtSyncLog::getTableMapperId, tableMapperId)
                    .eq(RtSyncLog::getSyncDate, syncDate)
                    .eq(RtSyncLog::getSyncHour, syncHour);
            RtSyncLog row = rtSyncLogMapper.selectOne(q);
            if (row == null) {
                return out;
            }
            out.put("insert_data_volume", defaultLong(row.getInsertDataVolume()));
            out.put("update_data_volume", defaultLong(row.getUpdateDataVolume()));
            out.put("delete_data_volume", defaultLong(row.getDeleteDataVolume()));
        } catch (Exception e) {
            log.warn("从 rt_sync_log 读取按小时统计失败, taskId={}, tableMapperId={}, date={}, hour={}",
                    taskId, tableMapperId, syncDate, syncHour, e);
        }
        return out;
    }

    private static Map<String, Long> emptyHourlyStatsMap() {
        Map<String, Long> m = new HashMap<>(3);
        m.put("insert_data_volume", 0L);
        m.put("update_data_volume", 0L);
        m.put("delete_data_volume", 0L);
        return m;
    }

    private static long defaultLong(Long v) {
        return v == null ? 0L : v;
    }

    /**
     * 获取数据同步任务真实状态
     *
     * @param id 任务ID
     * @return 数据同步任务真实状态
     */
    @Override
    public RealTimeSyncTaskStatus getTaskRealStatus(Integer id) {
        RtTask rtTask = getRtTaskByTaskId(id);
        DataSyncTask task = buildDataSyncTask(rtTask);
        RealTimeDataSyncEngine realTimeDataSyncEngine = new RealTimeDataSyncEngine(kafkaDataProperties.getBrokers(), kafkaDataProperties.getConnector());
        return realTimeDataSyncEngine.status(task);
    }

    /**
     * 转换为字段映射VO
     *
     * @param fm
     * @return
     */
    private RealTimeFieldMapperVo convertToFieldMapperVo(RtFieldMapper fm) {
        RealTimeFieldMapperVo vo = new RealTimeFieldMapperVo();
        vo.setId(fm.getId());
        vo.setSourceFieldId(fm.getSourceFieldId());
        vo.setSourceFieldName(fm.getSourceFieldName());
        vo.setTargetFieldId(fm.getTargetFieldId());
        vo.setTargetFieldName(fm.getTargetFiledName());
        return vo;
    }

    /**
     * 获取实时数据同步任务
     *
     * @param taskId 实时数据同步任务ID
     * @return 实时数据同步任务
     */
    private RtTask getRtTaskByTaskId(Integer taskId) {
        LambdaQueryWrapper<RtTask> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(RtTask::getId, taskId)
                .eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        RtTask rtTask = rtTaskMapper.selectOne(queryWrapper);
        if (rtTask == null) {
            throw new BusinessException("请校验实时数据同步任务是否存在!");
        }
        return rtTask;
    }

    /**
     * 验证实时数据同步任务是否存在
     *
     * @param taskId 实时数据同步任务ID
     */
    private void verifyTaskExist(Integer taskId) {
        LambdaQueryWrapper<RtTask> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(RtTask::getId, taskId)
                .eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        RtTask task = rtTaskMapper.selectOne(queryWrapper);
        if (task == null) {
            throw new BusinessException("请校验实时数据同步任务是否存在!");
        }
    }

    /**
     * 验证实时数据同步任务是否存在
     *
     * @param sourceDbId 源数据库ID
     * @param targetDbId 目标数据库ID
     */
    private void verifyTaskExist(Integer sourceDbId, Integer targetDbId) {
        LambdaQueryWrapper<RtTask> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(RtTask::getSourceDbId, sourceDbId)
                .eq(RtTask::getTargetDbId, targetDbId)
                .eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        RtTask task = rtTaskMapper.selectOne(queryWrapper);
        if (task != null) {
            throw new BusinessException("当前实时任务已存在!");
        }
    }

    /**
     * 验证实时数据同步任务表单
     *
     * @param taskForm 实时数据同步任务表单
     */
    private void validateRealTimeTaskForm(RealTimeTaskForm taskForm) {
        if (CollectionUtils.isEmpty(taskForm.getTableMappers())) {
            throw new BusinessException("请配置实时数据同步表映射列表!");
        }
        for (RealTimeTableForm tableMappers : taskForm.getTableMappers()) {
            List<RealTimeFieldMapperForm> fieldMappers = tableMappers.getFieldMappers();
            if (CollectionUtils.isEmpty(fieldMappers)) {
                throw new BusinessException("请配置实时数据同步字段映射列表!");
            }
        }
    }
}