package com.pufferfishscheduler.worker.task.trans.engine.listener;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.worker.task.trans.engine.DataTransEngine;
import com.pufferfishscheduler.worker.task.trans.engine.TransWrapper;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobEntryExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingData;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.engine.logchannel.LogChannelManager;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.di.trans.step.StepMeta;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.List;

/**
 * kettle步骤日志监听器
 * 用于监听转换作业中每个步骤的激活和完成事件，记录步骤执行日志和统计信息。
 *
 * @author Mayc
 */
@Slf4j
@RequiredArgsConstructor
public class KettleStepListener implements StepListener {

    private final String logChannelId;
    private final DataTransEngine dataTransEngine;

    /**
     * 步骤激活事件
     *
     * @param trans    转换对象
     * @param stepMeta 步骤元数据
     * @param step     步骤对象
     */
    @Override
    public void stepActive(Trans trans, StepMeta stepMeta, StepInterface step) {
        log.info("步骤 {} 激活", stepMeta.getName());
    }

    /**
     * 步骤完成事件
     *
     * @param trans    转换对象
     * @param stepMeta 步骤元数据
     * @param step     步骤对象
     */
    @Override
    public void stepFinished(Trans trans, StepMeta stepMeta, StepInterface step) {
        TransWrapper transWrapper = (TransWrapper) trans;

        try {
            // 验证参数
            if (!validateParameters(trans, step)) {
                return;
            }

            String instanceId = transWrapper.getInstanceId();
            String stepName = step.getStepname();
            Date endTime = new Date();

            // 收集步骤执行统计信息
            StepStatistics statistics = collectStepStatistics(step);
            boolean executeResult = statistics.getNrLinesError() == 0;

            // 记录步骤执行结果
            logStepExecutionResult(stepName, instanceId, statistics, executeResult, endTime);

            // 更新作业执行信息
            updateJobExecutingInfo(instanceId, stepName, endTime, statistics, executeResult);

            // 更新步骤状态到日志通道
            updateLogChannel(stepName, executeResult, statistics);

            // 处理转换完成逻辑（整次转换结束时由 KettleTransListener 统一清理资源，此处切勿按「每步」清理）
            handleTransCompletion(transWrapper, instanceId);
        } catch (Exception e) {
            log.error("处理步骤完成事件失败", e);
        }
    }

    /**
     * 验证参数
     *
     * @param trans 转换对象
     * @param step  步骤对象
     * @return 参数是否有效
     */
    private boolean validateParameters(Trans trans, StepInterface step) {
        if (trans == null) {
            log.error("转换对象为空");
            return false;
        }
        if (!(trans instanceof TransWrapper)) {
            log.error("转换对象不是TransWrapper类型");
            return false;
        }
        if (step == null) {
            log.error("步骤对象为空");
            return false;
        }
        if (logChannelId == null) {
            log.error("日志通道ID为空");
            return false;
        }
        return true;
    }

    /**
     * 收集步骤执行统计信息
     *
     * @param step 步骤对象
     * @return 步骤统计信息
     */
    private StepStatistics collectStepStatistics(StepInterface step) {
        return new StepStatistics(
                step.getLinesRead(),
                step.getLinesWritten(),
                step.getLinesInput(),
                step.getLinesUpdated(),
                step.getLinesOutput(),
                step.getLinesRejected(),
                step.getErrors(),
                0L, // nrLinesDeleted
                0L, // nrReslutFiles
                0L  // nrResultRow
        );
    }

    /**
     * 记录步骤执行结果
     *
     * @param stepName      步骤名称
     * @param instanceId    实例ID
     * @param statistics    统计信息
     * @param executeResult 执行结果
     * @param endTime       结束时间
     */
    private void logStepExecutionResult(String stepName, String instanceId,
                                        StepStatistics statistics, boolean executeResult,
                                        Date endTime) {
        log.info("步骤 {} 执行完成，实例ID: {}, 结果: {}, 读: {}, 写: {}, 更新: {}, 删除: {}, 输入: {}, 输出: {}, 拒绝: {}, 错误: {}, 结束时间: {}",
                stepName, instanceId, executeResult, statistics.getNrLinesRead(),
                statistics.getNrLinesWrite(), statistics.getNrLinesUpdated(), statistics.getNrLinesDeleted(),
                statistics.getNrLinesInput(), statistics.getNrLinesOutput(), statistics.getNrLinesRejected(),
                statistics.getNrLinesError(), endTime);
    }

    /**
     * 更新作业执行信息
     *
     * @param instanceId    实例ID
     * @param stepName      步骤名称
     * @param endTime       结束时间
     * @param statistics    统计信息
     * @param executeResult 执行结果
     */
    private void updateJobExecutingInfo(String instanceId, String stepName, Date endTime,
                                        StepStatistics statistics, boolean executeResult) {
        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo != null) {
            JobEntryExecutingInfo jobEntryExecutingInfo = createJobEntryExecutingInfo(
                    instanceId, stepName, endTime, statistics, executeResult);
            jobExecutingInfo.getJobEntryExcutinginfos().add(jobEntryExecutingInfo);
        }
    }

    /**
     * 创建作业条目执行信息
     *
     * @param instanceId    实例ID
     * @param stepName      步骤名称
     * @param endTime       结束时间
     * @param statistics    统计信息
     * @param executeResult 执行结果
     * @return 作业条目执行信息
     */
    private JobEntryExecutingInfo createJobEntryExecutingInfo(String instanceId, String stepName,
                                                              Date endTime, StepStatistics statistics,
                                                              boolean executeResult) {
        JobEntryExecutingInfo info = new JobEntryExecutingInfo();
        info.setInstanceId(instanceId);
        info.setEntryName(stepName);
        info.setEndTime(endTime);
        info.setResult(executeResult);
        info.setNrLinesRead(statistics.getNrLinesRead());
        info.setNrLinesWrite(statistics.getNrLinesWrite());
        info.setNrLinesUpdated(statistics.getNrLinesUpdated());
        info.setNrLinesRejected(statistics.getNrLinesRejected());
        info.setNrLinesError(statistics.getNrLinesError());
        info.setNrLinesDeleted(statistics.getNrLinesDeleted());
        info.setNrReslutFiles(statistics.getNrReslutFiles());
        info.setNrResultRow(statistics.getNrResultRow());
        info.setNrLinesInput(statistics.getNrLinesInput());
        info.setNrLinesOutput(statistics.getNrLinesOutput());
        return info;
    }

    /**
     * 更新日志通道
     *
     * @param stepName      步骤名称
     * @param executeResult 执行结果
     * @param statistics    统计信息
     */
    private void updateLogChannel(String stepName, boolean executeResult, StepStatistics statistics) {
        if (executeResult) {
            LogChannelManager.addLogIfPresent(logChannelId, Constants.EXECUTE_STATUS.SUCCESS,
                    String.format("步骤“%s”执行成功！输入：%s， 输出：%s， 读：%s， 写：%s， 更新：%s， 删除：%s， 拒绝：%s， 错误：%s",
                            stepName, statistics.getNrLinesInput(), statistics.getNrLinesOutput(),
                            statistics.getNrLinesRead(), statistics.getNrLinesWrite(),
                            statistics.getNrLinesUpdated(), statistics.getNrLinesDeleted(),
                            statistics.getNrLinesRejected(), statistics.getNrLinesError()));
        } else {
            LogChannelManager.addLogIfPresent(logChannelId, Constants.EXECUTE_STATUS.FAILURE,
                    String.format("步骤“%s”执行失败！", stepName));
        }

        LogChannelManager.addStepStatusIfPresent(logChannelId, stepName,
                executeResult ? Constants.EXECUTE_STATUS.SUCCESS : Constants.EXECUTE_STATUS.FAILURE);
    }

    /**
     * 处理转换完成逻辑
     *
     * @param transWrapper 转换包装器
     * @param instanceId   实例ID
     */
    private void handleTransCompletion(TransWrapper transWrapper, String instanceId) {
        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo != null && jobExecutingInfo.getFinished()) {
            try {
                calculateJobStatistics(jobExecutingInfo);
                transWrapper.setDataVolume(jobExecutingInfo.getNrLinesOutput());
                logJobExecutionResult(transWrapper, instanceId, jobExecutingInfo);
                handleJobResult(transWrapper, jobExecutingInfo);
            } catch (Exception e) {
                log.error("处理转换完成逻辑失败，实例ID: {}", instanceId, e);
            } finally {
                if (dataTransEngine.getTransTaskLogService() != null) {
                    dataTransEngine.getTransTaskLogService().syncTransTaskLog(jobExecutingInfo);
                }
            }
        }
    }

    /**
     * 计算作业统计信息
     *
     * @param jobExecutingInfo 作业执行信息
     */
    private void calculateJobStatistics(JobExecutingInfo jobExecutingInfo) {
        List<JobEntryExecutingInfo> entries = jobExecutingInfo.getJobEntryExcutinginfos();
        if (entries == null || entries.isEmpty()) {
            return;
        }

        long totalLinesRead = getLongValue(jobExecutingInfo.getNrLinesRead());
        long totalLinesWrite = getLongValue(jobExecutingInfo.getNrLinesWrite());
        long totalLinesInput = getLongValue(jobExecutingInfo.getNrLinesInput());
        long totalLinesUpdated = getLongValue(jobExecutingInfo.getNrLinesUpdated());
        long totalLinesOutput = getLongValue(jobExecutingInfo.getNrLinesOutput());
        long totalLinesRejected = getLongValue(jobExecutingInfo.getNrLinesRejected());
        long totalLinesError = getLongValue(jobExecutingInfo.getNrLinesError());
        long totalLinesDeleted = getLongValue(jobExecutingInfo.getNrLinesDeleted());

        for (JobEntryExecutingInfo entry : entries) {
            totalLinesRead += getLongValue(entry.getNrLinesRead());
            totalLinesWrite += getLongValue(entry.getNrLinesWrite());
            totalLinesInput += getLongValue(entry.getNrLinesInput());
            totalLinesUpdated += getLongValue(entry.getNrLinesUpdated());
            totalLinesOutput += getLongValue(entry.getNrLinesOutput());
            totalLinesRejected += getLongValue(entry.getNrLinesRejected());
            totalLinesError += getLongValue(entry.getNrLinesError());
            totalLinesDeleted += getLongValue(entry.getNrLinesDeleted());

            // 任何一个步骤执行失败，转换执行失败
            if (!entry.isResult()) {
                jobExecutingInfo.setResult(false);
            }
        }

        jobExecutingInfo.setNrLinesRead(totalLinesRead);
        jobExecutingInfo.setNrLinesWrite(totalLinesWrite);
        jobExecutingInfo.setNrLinesInput(totalLinesInput);
        jobExecutingInfo.setNrLinesOutput(totalLinesOutput);
        jobExecutingInfo.setNrLinesUpdated(totalLinesUpdated);
        jobExecutingInfo.setNrLinesRejected(totalLinesRejected);
        jobExecutingInfo.setNrLinesError(totalLinesError);
        jobExecutingInfo.setNrLinesDeleted(totalLinesDeleted);
    }

    /**
     * 获取Long值，如果为null则返回0
     *
     * @param value Long值
     * @return 非null的Long值
     */
    private long getLongValue(Long value) {
        return value != null ? value : 0L;
    }

    /**
     * 记录作业执行结果
     *
     * @param transWrapper     转换包装器
     * @param instanceId       实例ID
     * @param jobExecutingInfo 作业执行信息
     */
    private void logJobExecutionResult(TransWrapper transWrapper, String instanceId,
                                       JobExecutingInfo jobExecutingInfo) {
        log.info("转换 {} 执行完成，实例ID: {}, 参数: {}, 结果: {}, 读: {}, 写: {}, 更新: {}, 删除: {}, 输入: {}, 输出: {}, 拒绝: {}, 错误: {}, 开始时间: {}, 结束时间: {}",
                transWrapper.getName(), instanceId, jobExecutingInfo.getParameters(),
                jobExecutingInfo.getResult(), jobExecutingInfo.getNrLinesRead(),
                jobExecutingInfo.getNrLinesWrite(), jobExecutingInfo.getNrLinesUpdated(),
                jobExecutingInfo.getNrLinesDeleted(), jobExecutingInfo.getNrLinesInput(),
                jobExecutingInfo.getNrLinesOutput(), jobExecutingInfo.getNrLinesRejected(),
                jobExecutingInfo.getNrLinesError(), jobExecutingInfo.getBeginTime(),
                jobExecutingInfo.getEndTime());
    }

    /**
     * 处理作业结果
     *
     * @param transWrapper     转换包装器
     * @param jobExecutingInfo 作业执行信息
     */
    private void handleJobResult(TransWrapper transWrapper, JobExecutingInfo jobExecutingInfo) {
        if (getLongValue(jobExecutingInfo.getNrLinesError()) > 0 || (jobExecutingInfo.getResult() != null && !jobExecutingInfo.getResult())) {
            transWrapper.setExecuteStatus(false);
            jobExecutingInfo.setResult(false);
            jobExecutingInfo.setReason(jobExecutingInfo.getLogText());
            LogChannelManager.addLogIfPresent(logChannelId, Constants.EXECUTE_STATUS.FAILURE, "转换执行失败！");
        }
    }

    /**
     * 步骤统计信息
     */
    private static class StepStatistics {
        private final long nrLinesRead;
        private final long nrLinesWrite;
        private final long nrLinesInput;
        private final long nrLinesUpdated;
        private final long nrLinesOutput;
        private final long nrLinesRejected;
        private final long nrLinesError;
        private final long nrLinesDeleted;
        private final long nrReslutFiles;
        private final long nrResultRow;

        public StepStatistics(long nrLinesRead, long nrLinesWrite, long nrLinesInput,
                              long nrLinesUpdated, long nrLinesOutput, long nrLinesRejected,
                              long nrLinesError, long nrLinesDeleted, long nrReslutFiles,
                              long nrResultRow) {
            this.nrLinesRead = nrLinesRead;
            this.nrLinesWrite = nrLinesWrite;
            this.nrLinesInput = nrLinesInput;
            this.nrLinesUpdated = nrLinesUpdated;
            this.nrLinesOutput = nrLinesOutput;
            this.nrLinesRejected = nrLinesRejected;
            this.nrLinesError = nrLinesError;
            this.nrLinesDeleted = nrLinesDeleted;
            this.nrReslutFiles = nrReslutFiles;
            this.nrResultRow = nrResultRow;
        }

        public long getNrLinesRead() {
            return nrLinesRead;
        }

        public long getNrLinesWrite() {
            return nrLinesWrite;
        }

        public long getNrLinesInput() {
            return nrLinesInput;
        }

        public long getNrLinesUpdated() {
            return nrLinesUpdated;
        }

        public long getNrLinesOutput() {
            return nrLinesOutput;
        }

        public long getNrLinesRejected() {
            return nrLinesRejected;
        }

        public long getNrLinesError() {
            return nrLinesError;
        }

        public long getNrLinesDeleted() {
            return nrLinesDeleted;
        }

        public long getNrReslutFiles() {
            return nrReslutFiles;
        }

        public long getNrResultRow() {
            return nrResultRow;
        }
    }
}

