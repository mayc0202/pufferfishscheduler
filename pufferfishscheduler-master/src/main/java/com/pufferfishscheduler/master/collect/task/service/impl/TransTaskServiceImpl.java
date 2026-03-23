package com.pufferfishscheduler.master.collect.task.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.Enable;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskMapper;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.domain.form.collect.TransTaskForm;
import com.pufferfishscheduler.domain.vo.collect.TransTaskVo;
import com.pufferfishscheduler.master.collect.group.service.TransGroupService;
import com.pufferfishscheduler.master.collect.task.service.TransTaskService;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import com.pufferfishscheduler.master.dispatch.kafka.TaskDispatchKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 转换任务服务实现
 *
 * @author Mayc
 * @since 2026-03-20
 */
@Slf4j
@Service
public class TransTaskServiceImpl implements TransTaskService {

    @Autowired
    private TransTaskMapper transTaskMapper;

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private TransGroupService transGroupService;

    @Autowired
    private DictService dictService;

    @Autowired
    private TaskDispatchKafkaProducer taskDispatchKafkaProducer;

    /**
     * 与 {@link com.pufferfishscheduler.master.dispatch.schedule.TransKafkaScheduleScanner}、元数据立即同步使用同一语义：卡住后才允许抢占
     */
    @Value("${scheduler.trans.stuck-threshold-ms:300000}")
    private long stuckThresholdMs;

    /**
     * 分页查询转换任务
     *
     * @param groupId  分组id
     * @param name     任务名称
     * @param status   任务状态
     * @param enable   是否启用
     * @param pageNo   页码
     * @param pageSize 每页数量
     * @return 分页结果
     */
    @Override
    public IPage<TransTaskVo> list(Integer groupId, String name, String status, Boolean enable,
                                   Integer pageNo, Integer pageSize) {
        Map<String, Object> params = new HashMap<>(8);
        if (groupId != null) {
            List<Integer> groupIds = transGroupService.getAllChildGroupIds(groupId);
            params.put("groupIds", groupIds);
        }
        if (StringUtils.isNotBlank(name)) {
            params.put("name", name.trim());
        }
        if (StringUtils.isNotBlank(status)) {
            params.put("status", status);
        }
        if (enable != null) {
            params.put("enable", enable);
        }

        Page<TransTaskVo> page = new Page<>(pageNo, pageSize);
        IPage<TransTaskVo> result = transTaskMapper.selectTaskList(page, params);
        result.getRecords().forEach(this::fillDictText);
        return result;
    }

    /**
     * 任务详情（含字典翻译）
     */
    @Override
    public TransTaskVo detail(Integer id) {
        TransTaskVo vo = transTaskMapper.selectTaskById(id);
        if (vo == null) {
            throw new BusinessException(String.format("任务id为[%s]的任务不存在!", id));
        }
        fillDictText(vo);
        return vo;
    }

    /**
     * 新增转换任务
     *
     * @param taskForm 转换任务表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void add(TransTaskForm taskForm) {
        validateForm(taskForm, false);
        verifyFlowExists(taskForm.getFlowId());
        verifyTaskNameExists(taskForm.getName(), null);

        TransTask task = new TransTask();
        BeanUtils.copyProperties(taskForm, task);
        task.setDeleted(Constants.DELETE_FLAG.FALSE);
        task.setStatus(Constants.JOB_MANAGE_STATUS.INIT);
        task.setEnable(taskForm.getEnable());
        task.setCreatedBy(UserContext.getCurrentAccount());
        task.setCreatedTime(new Date());
        transTaskMapper.insert(task);
    }

    /**
     * 更新转换任务
     *
     * @param taskForm 转换任务表单
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void update(TransTaskForm taskForm) {
        if (taskForm.getId() == null) {
            throw new BusinessException("任务ID不能为空!");
        }
        validateForm(taskForm, true);
        TransTask exist = getTransTaskById(taskForm.getId());
        verifyFlowExists(taskForm.getFlowId());
        verifyTaskNameExists(taskForm.getName(), taskForm.getId());

        BeanUtils.copyProperties(taskForm, exist);
        exist.setEnable(taskForm.getEnable());
        exist.setStatus(Constants.JOB_MANAGE_STATUS.INIT);
        exist.setReason("");
        exist.setUpdatedBy(UserContext.getCurrentAccount());
        exist.setUpdatedTime(new Date());
        transTaskMapper.updateById(exist);
    }

    /**
     * 删除转换任务（逻辑删除）
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(Integer id) {
        getTransTaskById(id);
        UpdateWrapper<TransTask> uw = new UpdateWrapper<>();
        uw.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());
        transTaskMapper.update(null, uw);
    }

    /**
     * 启用转换任务
     *
     * @param id
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void enable(Integer id) {
        TransTask task = getTransTaskById(id);
        task.setEnable(true);
        task.setUpdatedBy(UserContext.getCurrentAccount());
        task.setUpdatedTime(new Date());
        transTaskMapper.updateById(task);
    }

    /**
     * 禁用转换任务
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void disable(Integer id) {
        TransTask task = getTransTaskById(id);
        task.setEnable(false);
        task.setUpdatedBy(UserContext.getCurrentAccount());
        task.setUpdatedTime(new Date());
        transTaskMapper.updateById(task);
    }

    /**
     * 立即执行转换任务：抢占为 STARTING、写入计划 execute_time（秒级），投递 Kafka，由 Worker 消费执行（与元数据立即同步一致）
     */
    @Override
    public void immediatelyExecute(Integer id) {
        TransTask task = getTransTaskById(id);
        if (!Boolean.TRUE.equals(task.getEnable())) {
            throw new BusinessException("任务未启用，无法立即执行!");
        }
        if (task.getStatus() == null) {
            throw new BusinessException("任务状态异常，无法立即执行!");
        }
        if (Constants.JOB_MANAGE_STATUS.STOP.equals(task.getStatus())
                || Constants.JOB_MANAGE_STATUS.STOPPING.equals(task.getStatus())) {
            throw new BusinessException("任务已停止，无法立即执行!");
        }
        if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(task.getStatus())
                && "1".equals(task.getFailurePolicy())) {
            throw new BusinessException("任务已失败且失败策略为停止，无法立即执行!");
        }

        Date now = new Date();
        Date scheduledDate = new Date((now.getTime() / 1000) * 1000);

        UpdateWrapper<TransTask> uw = new UpdateWrapper<>();
        uw.eq("id", task.getId())
                .eq("deleted", Constants.DELETE_FLAG.FALSE);

        boolean inProgress = Constants.JOB_MANAGE_STATUS.STARTING.equals(task.getStatus())
                || Constants.JOB_MANAGE_STATUS.RUNNING.equals(task.getStatus());
        if (inProgress) {
            if (task.getExecuteTime() == null) {
                throw new BusinessException("任务正在执行中，请稍后重试!");
            }
            long diff = now.getTime() - task.getExecuteTime().getTime();
            if (diff <= stuckThresholdMs) {
                throw new BusinessException("任务正在执行中，请稍后再试!");
            }
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

        int updated = transTaskMapper.update(null, uw);
        if (updated != 1) {
            throw new BusinessException("立即执行失败，任务状态可能已变更，请刷新后重试!");
        }

        TaskDispatchMessage msg = new TaskDispatchMessage();
        msg.setTaskType(Constants.TASK_TYPE.TRANS_TASK);
        msg.setTaskId(task.getId());
        msg.setScheduledTimeMillis(scheduledDate.getTime());
        taskDispatchKafkaProducer.send(msg);
        log.info("转换任务已投递立即执行, taskId={}, scheduledTimeMillis={}", id, scheduledDate.getTime());
    }

    /**
     * 立即停止转换任务：抢占为 STOPPING、写入本次停止派发的 execute_time（秒级），投递 Kafka，由 Worker 消费并尝试中断 Kettle。
     */
    @Override
    public void immediatelyStop(Integer id) {
        TransTask task = getTransTaskById(id);
        String status = task.getStatus();
        if (!Constants.JOB_MANAGE_STATUS.STARTING.equals(status)
                && !Constants.JOB_MANAGE_STATUS.RUNNING.equals(status)) {
            throw new BusinessException("当前任务未在运行或启动中，无法停止!");
        }

        Date now = new Date();
        Date scheduledDate = new Date((now.getTime() / 1000) * 1000);

        UpdateWrapper<TransTask> uw = new UpdateWrapper<>();
        uw.eq("id", task.getId())
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .in("status", Arrays.asList(
                        Constants.JOB_MANAGE_STATUS.STARTING,
                        Constants.JOB_MANAGE_STATUS.RUNNING))
                .set("status", Constants.JOB_MANAGE_STATUS.STOPPING)
                .set("execute_time", scheduledDate)
                .set("reason", "")
                .set("updated_time", now)
                .set("updated_by", UserContext.getCurrentAccount());

        int updated = transTaskMapper.update(null, uw);
        if (updated != 1) {
            throw new BusinessException("立即停止失败，任务状态可能已变更，请刷新后重试!");
        }

        TaskDispatchMessage msg = new TaskDispatchMessage();
        msg.setTaskType(Constants.TASK_TYPE.TRANS_TASK_STOP);
        msg.setTaskId(task.getId());
        msg.setScheduledTimeMillis(scheduledDate.getTime());
        taskDispatchKafkaProducer.send(msg);
        log.info("转换任务已投递立即停止, taskId={}, scheduledTimeMillis={}", id, scheduledDate.getTime());
    }

    /**
     * 验证转换任务表单
     */
    private void validateForm(TransTaskForm form, boolean update) {
        if (Constants.EXECUTE_TYPE.TIMING.equals(form.getExecuteType())
                && StringUtils.isBlank(form.getCron())) {
            throw new BusinessException("定时调度时 cron 表达式不能为空!");
        }
    }

    /**
     * 验证流程是否存在
     */
    private void verifyFlowExists(Integer flowId) {
        TransFlow flow = transFlowMapper.selectById(flowId);
        if (flow == null || Boolean.TRUE.equals(flow.getDeleted())) {
            throw new BusinessException(String.format("流程id[%s]不存在或已删除!", flowId));
        }
    }

    /**
     * 验证任务名称是否存在
     */
    private void verifyTaskNameExists(String name, Integer excludeId) {
        if (StringUtils.isBlank(name)) {
            return;
        }
        LambdaQueryWrapper<TransTask> q = new LambdaQueryWrapper<>();
        q.eq(TransTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        q.eq(TransTask::getName, name);
        if (excludeId != null) {
            q.ne(TransTask::getId, excludeId);
        }
        if (transTaskMapper.selectCount(q) > 0) {
            throw new BusinessException("任务名称已存在!");
        }
    }

    /**
     * 获取转换任务详情
     */
    private TransTask getTransTaskById(Integer id) {
        TransTask task = transTaskMapper.selectById(id);
        if (task == null || Boolean.TRUE.equals(task.getDeleted())) {
            throw new BusinessException(String.format("任务id为[%s]的任务不存在!", id));
        }
        return task;
    }

    /**
     * 填充字典文本
     */
    private void fillDictText(TransTaskVo vo) {
        vo.setStatusTxt(dictService.getDictItemValue(Constants.DICT.JOB_MANAGE_STATUS, vo.getStatus()));
        vo.setEnableTxt(dictService.getDictItemValue(Constants.DICT.ENABLE,
                vo.getEnable() ? Constants.ENABLE_FLAG.TRUE : Constants.ENABLE_FLAG.FALSE));
        vo.setNotifyPolicyTxt(dictService.getDictItemValue(Constants.DICT.NOTIFY_POLICY, vo.getNotifyPolicy()));
        vo.setFailurePolicyTxt(dictService.getDictItemValue(Constants.DICT.FAILURE_POLICY, vo.getFailurePolicy()));
        vo.setExecuteTypeTxt(dictService.getDictItemValue(Constants.DICT.SCHEDULE_TYPE, vo.getExecuteType()));
        vo.setExecuteTimeTxt(DateUtil.formatDateTime(vo.getExecuteTime()));
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));
    }
}
