package com.pufferfishscheduler.worker.task.trans.engine.listener;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.worker.task.trans.engine.DataTransEngine;
import com.pufferfishscheduler.worker.task.trans.engine.TransWrapper;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingData;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.engine.entity.LogText;
import com.pufferfishscheduler.worker.task.trans.engine.logchannel.LogChannelManager;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.BufferLine;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransListener;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * kettle转换流监听器
 * 用于监听转换作业的启动、激活和完成事件，记录转换作业执行日志和统计信息。
 *
 * @author Mayc
 */
@Slf4j
@AllArgsConstructor
public class KettleTransListener implements TransListener {

    /**
     * 不阻塞 Kettle 完成回调线程，避免固定 sleep 拖慢整库转换收尾。
     */
    private static final ExecutorService KETTLE_TRANS_CLEANUP = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "kettle-trans-cleanup");
        t.setDaemon(true);
        return t;
    });

    private final String logChannelId;
    private final DataTransEngine dataTransEngine;

    @Override
    public void transStarted(Trans trans) throws KettleException {
        TransWrapper transWrapper = (TransWrapper) trans;
        String instanceId = transWrapper.getInstanceId();

        log.info(String.format("★★★Begin to execute trans:%s , InstanceId is:%s",
                transWrapper.getName(), instanceId));

        JobExecutingInfo jobExecutingInfo = createJobExecutingInfo(transWrapper);
        JobExecutingData.put(instanceId, jobExecutingInfo);
    }

    @Override
    public void transActive(Trans trans) {
        // 可以在这里添加转换激活时的处理逻辑
    }

    @Override
    public void transFinished(Trans trans) throws KettleException {
        TransWrapper transWrapper = (TransWrapper) trans;
        String instanceId = transWrapper.getInstanceId();

        boolean isSuccess = true;
        String errorReason = "";
        String transLogChannelId = null;

        try {
            // 获取执行结果
            Result result = transWrapper.getResult();
            boolean executeResult = result.getResult();

            // 获取参数信息
            String parameters = getParameters(transWrapper);

            // 获取日志通道ID和日志内容
            transLogChannelId = transWrapper.getLogChannelId();
            String logText = getLogText(transLogChannelId);

            // 获取或创建作业执行信息
            JobExecutingInfo jobExecutingInfo = getOrCreateJobExecutingInfo(transWrapper, instanceId);
            updateJobExecutingInfo(jobExecutingInfo, transWrapper, executeResult, logText, parameters);

            // 处理错误日志
            processErrorLogs();

            // 记录执行完成日志
            logExecutionComplete(transWrapper, instanceId, parameters, executeResult, jobExecutingInfo);

        } catch (Exception e) {
            isSuccess = handleExecutionException(instanceId, e);
            errorReason = e.getMessage();
        } finally {
            // 处理无步骤的转换流程
            handleNoStepsTrans(trans, isSuccess, errorReason);

            // 清理运行中的转换和日志
            cleanupRunningTransAndLog(transWrapper);
        }
    }

    /**
     * 创建作业执行信息
     *
     * @param transWrapper 转换包装器
     * @return 作业执行信息
     */
    private JobExecutingInfo createJobExecutingInfo(TransWrapper transWrapper) {
        JobExecutingInfo jobExecutingInfo = new JobExecutingInfo();
        jobExecutingInfo.setInstanceId(transWrapper.getInstanceId());
        jobExecutingInfo.setTaskFlowLogId(transWrapper.getTaskFlowLogId());
        jobExecutingInfo.setBeginTime(new Date());
        jobExecutingInfo.setDataJobId(transWrapper.getInstanceId());
        jobExecutingInfo.setBusinessNo(transWrapper.getBusinessNo());
        jobExecutingInfo.setBusinessType(transWrapper.getBusinessType());
        jobExecutingInfo.setParentInstanceId(transWrapper.getParentInstanceId());
        return jobExecutingInfo;
    }

    /**
     * 获取或创建作业执行信息
     *
     * @param transWrapper 转换包装器
     * @param instanceId   实例ID
     * @return 作业执行信息
     */
    private JobExecutingInfo getOrCreateJobExecutingInfo(TransWrapper transWrapper, String instanceId) {
        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo == null) {
            // 流程设置有问题，无法启动，创建新的执行信息
            jobExecutingInfo = createJobExecutingInfo(transWrapper);
            JobExecutingData.put(instanceId, jobExecutingInfo);
        }
        return jobExecutingInfo;
    }

    /**
     * 更新作业执行信息
     *
     * @param jobExecutingInfo 作业执行信息
     * @param transWrapper     转换包装器
     * @param executeResult    执行结果
     * @param logText          日志文本
     * @param parameters       参数信息
     */
    private void updateJobExecutingInfo(JobExecutingInfo jobExecutingInfo, TransWrapper transWrapper,
                                        boolean executeResult, String logText, String parameters) {
        jobExecutingInfo.setEndTime(new Date());
        jobExecutingInfo.setLogText(logText);

        // 设置转换名称
        String path = transWrapper.getRepositoryDirectory().getPath();
        if (path != null && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        String transName = String.format("%s/%s", path, transWrapper.getName());
        jobExecutingInfo.setJobName(transName);

        // 设置执行结果和状态
        jobExecutingInfo.setResult(executeResult);
        transWrapper.setExecuteStatus(executeResult);

        // 设置执行信息
        jobExecutingInfo.setExecutingServer(transWrapper.getExecutingServer());
        jobExecutingInfo.setExecutingUser(transWrapper.getExecutingUser());
        jobExecutingInfo.setExecutingType(transWrapper.getExecutingType());
        jobExecutingInfo.setFinished(true);
        jobExecutingInfo.setParameters(parameters);
    }

    /**
     * 获取日志文本
     *
     * @param transLogChannelId 转换日志通道ID
     * @return 日志文本
     */
    private String getLogText(String transLogChannelId) {
        LoggingBuffer appender = KettleLogStore.getAppender();
        return appender.getBuffer(transLogChannelId, true).toString();
    }

    /**
     * 处理错误日志
     */
    private void processErrorLogs() {
        LoggingBuffer appender = KettleLogStore.getAppender();
        Iterator<BufferLine> it = appender.getBufferIterator();

        while (it != null && it.hasNext()) {
            BufferLine bufferLine = it.next();
            KettleLoggingEvent event = bufferLine.getEvent();

            if (event.getLevel().isError()) {
                String errorMessage = event.getMessage().toString()
                        .replaceAll("(\\r\\n|\\r|\\n|\\n\\r)", "<br/>");

                LogChannelManager.addLogTextIfPresent(logChannelId,
                        new LogText(Constants.EXECUTE_STATUS.FAILURE, errorMessage));
            }
        }
    }

    /**
     * 记录执行完成日志
     *
     * @param transWrapper     转换包装器
     * @param instanceId       实例ID
     * @param parameters       参数信息
     * @param executeResult    执行结果
     * @param jobExecutingInfo 作业执行信息
     */
    private void logExecutionComplete(TransWrapper transWrapper, String instanceId,
                                      String parameters, boolean executeResult, JobExecutingInfo jobExecutingInfo) {
        log.info(String.format(
                "★★★Finish to execute trans:%s , \n InstanceId is:%s, \n paramters are:%s, \n result[ 状态：%s, beginTime:%s, endTime:%s]",
                transWrapper.getName(), instanceId, parameters,
                executeResult,
                jobExecutingInfo.getBeginTime(),
                jobExecutingInfo.getEndTime()));
    }

    /**
     * 处理执行异常
     *
     * @param instanceId 实例ID
     * @param e          异常
     * @return 是否成功
     */
    private boolean handleExecutionException(String instanceId, Exception e) {
        log.error("完成TransListener失败！InstanceId:" + instanceId, e);

        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo != null) {
            jobExecutingInfo.setFinished(true);
        }

        return false;
    }

    /**
     * 处理无步骤的转换流程
     *
     * @param trans       转换
     * @param isSuccess   是否成功
     * @param errorReason 错误原因
     */
    private void handleNoStepsTrans(Trans trans, boolean isSuccess, String errorReason) {
        // 存在一类流程，输入和输出之间连线被禁用掉了。导致流程不会走StepListner。导致流程一直无法终止
        if (trans.getSteps() == null || trans.getSteps().isEmpty()) {
            if (isSuccess) {
                LogChannelManager.addLogTextIfPresent(logChannelId,
                        new LogText(Constants.EXECUTE_STATUS.SUCCESS, "转换执行成功！"));
            } else {
                LogChannelManager.addLogTextIfPresent(logChannelId,
                        new LogText(Constants.EXECUTE_STATUS.FAILURE,
                                "转换执行失败！失败原因：" + StringUtils.defaultString(errorReason)));
            }

            LogChannelManager.setChannelStatusIfPresent(logChannelId,
                    isSuccess ? Constants.EXECUTE_STATUS.SUCCESS : Constants.EXECUTE_STATUS.FAILURE);
        }
    }

    /**
     * 清理运行中的转换和日志
     *
     * @param transWrapper 转换包装器
     */
    private void cleanupRunningTransAndLog(TransWrapper transWrapper) {
        final TransWrapper tw = transWrapper;
        KETTLE_TRANS_CLEANUP.execute(() -> {
            try {
                if (tw == null) {
                    return;
                }
                KettleLogStore.discardLines(tw.getLogChannelId(), true);

                JobExecutingInfo jobExecutingInfo = JobExecutingData.get(tw.getInstanceId());
                if (jobExecutingInfo != null) {
                    jobExecutingInfo.getJobEntryExcutinginfos().clear();
                    JobExecutingData.remove(tw.getInstanceId());
                }

                dataTransEngine.cleanRunningTransAndLog(tw);
            } catch (Exception e) {
                log.warn("Kettle 转换资源清理异常, instanceId={}",
                        tw != null ? tw.getInstanceId() : null, e);
            }
        });
    }

    /**
     * 获取参数信息
     *
     * @param trans 转换包装器
     * @return 参数信息JSON字符串
     */
    private String getParameters(TransWrapper trans) {
        String[] parameters = trans.listParameters();
        if (parameters == null || parameters.length == 0) {
            return "";
        }

        Map<String, Object> paramMap = new HashMap<>();
        for (String param : parameters) {
            try {
                String value = trans.getParameterDefault(param);
                if (!StringUtils.isEmpty(trans.getParameterValue(param))) {
                    value = trans.getParameterValue(param);
                }
                paramMap.put(param, value);
            } catch (UnknownParamException e) {
                log.error("Unknown Parameter: " + param, e);
            }
        }

        return JSONObject.toJSONString(trans.getParams());
    }

}
