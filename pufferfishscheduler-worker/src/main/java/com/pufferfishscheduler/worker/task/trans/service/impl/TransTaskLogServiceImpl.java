package com.pufferfishscheduler.worker.task.trans.service.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.dao.entity.TransTaskLog;
import com.pufferfishscheduler.dao.entity.TransTaskStepLog;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskLogMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskStepLogMapper;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobEntryExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.service.TransTaskLogService;
import com.pufferfishscheduler.worker.task.trans.util.LogContentSanitizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * 转换任务执行日志落库
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransTaskLogServiceImpl implements TransTaskLogService {

    private final TransTaskMapper transTaskMapper;
    private final TransFlowMapper transFlowMapper;
    private final TransTaskLogMapper transTaskLogMapper;
    private final TransTaskStepLogMapper transTaskStepLogMapper;

    @Override
    public void syncTransTaskLog(JobExecutingInfo jobExecutingInfo) {
        if (jobExecutingInfo == null) {
            return;
        }
        String runId = jobExecutingInfo.getInstanceId();
        if (StringUtils.isBlank(runId)) {
            log.warn("跳过保存任务执行日志：instanceId 为空");
            return;
        }

        try {
            TransTaskLog transTaskLog = buildTransTaskLog(jobExecutingInfo);
            transTaskLogMapper.insert(transTaskLog);

            List<TransTaskStepLog> steps = buildTransTaskStepLogList(jobExecutingInfo, runId);
            if (!steps.isEmpty()) {
                transTaskStepLogMapper.insertBatch(steps);
            }
            log.debug("任务执行日志已保存, runId={}, stepCount={}", runId, steps.size());
        } catch (Exception e) {
            log.error("保存任务执行日志失败, runId={}", runId, e);
        }
    }

    /**
     * 构建任务执行日志
     *
     * @param info 任务执行信息
     * @return 任务执行日志
     */
    private TransTaskLog buildTransTaskLog(JobExecutingInfo info) {
        TransTaskLog row = new TransTaskLog();
        row.setId(info.getInstanceId());

        long errLines = longOrZero(info.getNrLinesError());
        boolean failed = errLines > 0 || Boolean.FALSE.equals(info.getResult());
        String sanitizedLog = LogContentSanitizer.sanitizeForStorage(info.getLogText());

        row.setStatus(failed ? Constants.EXECUTE_STATUS.FAILURE : Constants.EXECUTE_STATUS.SUCCESS);
        if (failed) {
            row.setReason(sanitizedLog);
        }
        row.setLogDetail(sanitizedLog);

        applyExecutingType(row, info.getBusinessType());
        Integer taskId = parseTaskIdOrNull(info.getBusinessNo());
        if (taskId != null) {
            row.setTaskId(taskId);
        }

        row.setLinesRead(info.getNrLinesRead());
        row.setLinesWritten(info.getNrLinesWrite());
        row.setLinesUpdated(info.getNrLinesUpdated());
        row.setLinesInput(info.getNrLinesInput());
        row.setLinesOutput(info.getNrLinesOutput());
        row.setLinesRejected(info.getNrLinesRejected());
        row.setLinesErrors(info.getNrLinesError());
        row.setDataVolume(longOrZero(info.getNrLinesOutput()) + longOrZero(info.getNrLinesUpdated()));
        row.setStartTime(info.getBeginTime());
        row.setEndTime(info.getEndTime());
        row.setCreatedBy(Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        row.setFlowImage(resolveFlowImage(taskId));
        row.setParameters(LogContentSanitizer.sanitizeForStorage(info.getParameters()));
        row.setCreatedTime(new Date());
        return row;
    }

    /**
     * 解析任务执行日志中的流程图片
     *
     * @param taskId 任务ID
     * @return 流程图片
     */
    private String resolveFlowImage(Integer taskId) {
        if (taskId == null) {
            return null;
        }
        TransTask task = transTaskMapper.selectById(taskId);
        if (task == null || task.getFlowId() == null) {
            return null;
        }
        TransFlow flow = transFlowMapper.selectById(task.getFlowId());
        return flow != null ? flow.getImage() : null;
    }

    /**
     * 构建任务执行日志中的步骤日志
     *
     * @param info      任务执行信息
     * @param taskLogId 任务执行日志ID
     * @return 步骤日志列表
     */
    private List<TransTaskStepLog> buildTransTaskStepLogList(JobExecutingInfo info, String taskLogId) {
        List<JobEntryExecutingInfo> entries = info.getJobEntryExcutinginfos();
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }

        Date now = new Date();
        List<TransTaskStepLog> result = new ArrayList<>(entries.size());
        for (JobEntryExecutingInfo entry : entries) {
            result.add(toStepLog(entry, taskLogId, now));
        }
        return result;
    }

    /**
     * 构建任务执行日志中的步骤日志
     *
     * @param entry       任务执行信息
     * @param taskLogId   任务执行日志ID
     * @param createdTime 创建时间
     * @return 步骤日志
     */
    private static TransTaskStepLog toStepLog(JobEntryExecutingInfo entry, String taskLogId, Date createdTime) {
        TransTaskStepLog row = new TransTaskStepLog();
        row.setTaskLogId(taskLogId);
        row.setStepName(entry.getEntryName());
        row.setStatus(entry.isResult() ? Constants.EXECUTE_STATUS.SUCCESS : Constants.EXECUTE_STATUS.FAILURE);
        row.setReason(LogContentSanitizer.sanitizeForStorage(entry.getReason()));
        row.setLinesRead(entry.getNrLinesRead());
        row.setLinesWritten(entry.getNrLinesWrite());
        row.setLinesUpdated(entry.getNrLinesUpdated());
        row.setLinesInput(entry.getNrLinesInput());
        row.setLinesOutput(entry.getNrLinesOutput());
        row.setLinesRejected(entry.getNrLinesRejected());
        row.setLinesErrors(entry.getNrLinesError());
        row.setStartTime(entry.getBeginTime());
        row.setEndTime(entry.getEndTime());
        row.setCreatedBy(Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        row.setCreatedTime(createdTime);
        return row;
    }

    /**
     * 将Long转换为0或实际值
     *
     * @param v Long值
     * @return 0或实际值
     */
    private static long longOrZero(Long v) {
        return v != null ? v : 0L;
    }

    /**
     * 应用任务执行日志中的执行类型
     *
     * @param row          任务执行日志
     * @param businessType 业务类型
     */
    private void applyExecutingType(TransTaskLog row, String businessType) {
        if (StringUtils.isBlank(businessType)) {
            return;
        }
        try {
            row.setExecutingType(businessType);
        } catch (NumberFormatException ex) {
            log.warn("businessType 非数字，已跳过 executingType 写入: {}", businessType);
        }
    }

    /**
     * 解析任务执行日志中的任务ID
     *
     * @param businessNo 业务编号
     * @return 任务ID或null
     */
    private Integer parseTaskIdOrNull(String businessNo) {
        if (StringUtils.isBlank(businessNo)) {
            return null;
        }
        try {
            return Integer.valueOf(businessNo.trim());
        } catch (NumberFormatException ex) {
            log.warn("businessNo 非数字，已跳过 taskId 写入: {}", businessNo);
            return null;
        }
    }
}
