package com.pufferfishscheduler.worker.task.trans.service;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskMapper;
import com.pufferfishscheduler.trans.engine.DataTransEngine;
import com.pufferfishscheduler.trans.engine.TransWrapper;
import com.pufferfishscheduler.trans.engine.entity.TransParam;
import com.pufferfishscheduler.trans.engine.listener.KettleStepListener;
import com.pufferfishscheduler.trans.engine.listener.KettleStepRowListener;
import com.pufferfishscheduler.trans.engine.listener.KettleTransListener;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannel;
import com.pufferfishscheduler.trans.engine.logchannel.LogChannelManager;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepMetaConstructorFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * 转换任务服务
 *
 * @author Mayc
 * @since 2026-03-22  11:34
 */
@Slf4j
@Component
public class TransTaskExecutor {

    /**
     * 运行中的 taskId → Kettle 实例 instanceId，供立即停止时定位（{@link DataTransEngine#CACHE_TRANS_MAP} 按 instanceId 存储）。
     */
    private static final ConcurrentHashMap<Integer, String> RUNNING_TASK_TO_INSTANCE = new ConcurrentHashMap<>();

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private TransTaskMapper transTaskMapper;

    @Autowired
    DataTransEngine dataTransEngine;

    @Autowired
    @Qualifier("transTaskWorkExecutor")
    private Executor transTaskWorkExecutor;

    /**
     * Kafka / 调度侧调用：提交到线程池异步执行，完成后在池内线程更新 trans_task 状态。
     */
    public void submitExecute(Integer taskId) {
        transTaskWorkExecutor.execute(() -> {
            TransTask current = transTaskMapper.selectById(taskId);
            String failurePolicy = current != null ? current.getFailurePolicy() : "1";
            try {
                executeBlocking(taskId);
                markTransTaskSuccess(taskId);
            } catch (Exception e) {
                log.error("转换任务执行失败, taskId={}", taskId, e);
                markTransTaskFailed(taskId, failurePolicy, e);
            }
        });
    }

    /**
     * 单次运行的资源 id：taskId + runId，避免并发或连续触发时 LogChannel 互相覆盖。
     */
    private void executeBlocking(Integer taskId) throws Exception {
        log.info("执行转换任务, id={}", taskId);

        TransTask transTask = getTransTaskById(taskId);
        if (transTask == null) {
            throw new IllegalStateException("转换任务不存在, id=" + taskId);
        }

        TransFlow transFlow = getTransFlowById(transTask.getFlowId());
        if (transFlow == null) {
            String msg = "id为" + taskId + "的转换任务关联的转换流不存在!";
            persistFailureReason(taskId, msg);
            throw new IllegalStateException(msg);
        }

        String runId = UUID.randomUUID().toString().replace("-", "");
        String resourceId = taskId + "/" + runId;
        String logKey = LogChannelManager.getKey(DataTransEngine.ResourceType.TRANS.name(), resourceId);
        LogChannel logChannel = new LogChannel(DataTransEngine.ResourceType.TRANS.name(), resourceId, transFlow.getName());
        LogChannelManager.put(logKey, logChannel);
        logChannel.setStatus(Constants.EXECUTE_STATUS.RUNNING);

        try {
            syncExecute(transFlow, taskId, logChannel);
        } finally {
            LogChannelManager.remove(logKey);
        }
    }

    /**
     * 标记转换任务为成功
     *
     * @param taskId 转换任务ID
     */
    private void markTransTaskSuccess(Integer taskId) {
        Date finishedAt = new Date();
        UpdateWrapper<TransTask> toInit = new UpdateWrapper<>();
        toInit.eq("id", taskId)
                .eq("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                .set("status", Constants.JOB_MANAGE_STATUS.INIT)
                .set("reason", "")
                .set("updated_time", finishedAt)
                .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        int rows = transTaskMapper.update(null, toInit);
        if (rows == 0) {
            // 用户可能已点「立即停止」，Master 已将状态改为 STOPPING
            UpdateWrapper<TransTask> toStop = new UpdateWrapper<>();
            toStop.eq("id", taskId)
                    .eq("status", Constants.JOB_MANAGE_STATUS.STOPPING)
                    .set("status", Constants.JOB_MANAGE_STATUS.STOP)
                    .set("reason", "")
                    .set("updated_time", finishedAt)
                    .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            transTaskMapper.update(null, toStop);
        }
    }

    /**
     * 标记转换任务为失败
     *
     * @param taskId        转换任务ID
     * @param failurePolicy 失败策略
     * @param e             异常信息
     */
    private void markTransTaskFailed(Integer taskId, String failurePolicy, Exception e) {
        String finalStatus = "1".equals(failurePolicy)
                ? Constants.JOB_MANAGE_STATUS.STOP
                : Constants.JOB_MANAGE_STATUS.FAILURE;
        String reason = formatFailureReason(e);
        String reasonDb = reason.length() > 8000 ? reason.substring(0, 8000) : reason;
        Date finishedAt = new Date();
        UpdateWrapper<TransTask> toFail = new UpdateWrapper<>();
        toFail.eq("id", taskId)
                .eq("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                .set("status", finalStatus)
                .set("reason", reasonDb)
                .set("updated_time", finishedAt)
                .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        int rows = transTaskMapper.update(null, toFail);
        if (rows == 0) {
            UpdateWrapper<TransTask> stopping = new UpdateWrapper<>();
            stopping.eq("id", taskId)
                    .eq("status", Constants.JOB_MANAGE_STATUS.STOPPING)
                    .set("status", finalStatus)
                    .set("reason", reasonDb)
                    .set("updated_time", finishedAt)
                    .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            transTaskMapper.update(null, stopping);
        }
    }

    /**
     * 将失败原因写入 trans_task.reason（与调度侧 buildFailureReason 规则一致：优先 message，截断 8000）
     */
    private void persistFailureReason(Integer taskId, String reason) {
        if (taskId == null || StringUtils.isBlank(reason)) {
            return;
        }
        String r = reason.length() > 8000 ? reason.substring(0, 8000) : reason;
        Date now = new Date();
        UpdateWrapper<TransTask> uw = new UpdateWrapper<>();
        uw.eq("id", taskId)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("reason", r)
                .set("updated_time", now)
                .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        transTaskMapper.update(null, uw);
    }

    /**
     * 格式化失败原因，截断 8000 字符
     */
    private static String formatFailureReason(Throwable e) {
        if (e == null) {
            return "转换任务执行失败";
        }
        String msg = e.getMessage();
        if (StringUtils.isNotBlank(msg)) {
            return msg.length() > 8000 ? msg.substring(0, 8000) : msg;
        }
        return e.getClass().getSimpleName();
    }


    /**
     * 同步执行转换流
     *
     * @param transFlow  转换流
     * @param taskId     转换任务 ID（与流程定义解耦）
     * @param logChannel 日志通道
     */
    private void syncExecute(TransFlow transFlow, Integer taskId, LogChannel logChannel) throws Exception {
        String config = transFlow.getConfig();
        TransWrapper trans = null;
        boolean success = false;
        try {
            List<TransParam> params = new ArrayList<>();

            // 执行前置方法
            beforeTrans(transFlow.getId(), config, params);

            // 解析参数配置
            parseParamConfig(transFlow.getParamConfig(), params);

            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开始初始化开发流程！");

            // 与 LogChannelManager.put 使用同一复合 id（taskId/runId）
            String listenerKey = LogChannelManager.getKey(logChannel.getType(), logChannel.getId());
            trans = dataTransEngine.executeTrans(
                    transFlow,
                    taskId,
                    params,
                    new KettleTransListener(listenerKey),
                    new KettleStepListener(listenerKey),
                    new KettleStepRowListener(listenerKey));

            registerRunningTrans(taskId, trans.getInstanceId());
            try {
                logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开发流程初始化成功！");
                logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "开始执行开发流程...");

                // 等待转换执行完成
                trans.waitUntilFinished();
            } finally {
                unregisterRunningTrans(taskId, trans.getInstanceId());
            }

            if (trans.getErrors() > 0) {
                String errMsg = "Kettle 转换结束但存在错误，错误数=" + trans.getErrors();
                logChannel.addLog(Constants.EXECUTE_STATUS.FAILURE, errMsg);
                throw new IllegalStateException(errMsg);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.warn("线程被中断", e);
                Thread.currentThread().interrupt();
            }

            afterTrans(transFlow.getId(), config);
            success = true;

        } catch (Exception e) {
            log.error("执行转换流程失败，转换流程id：{}", transFlow.getId(), e);
            String detail = e.getMessage();
            logChannel.addLog(Constants.EXECUTE_STATUS.FAILURE,
                    "执行流程失败。原因：" + (StringUtils.isNotBlank(detail) ? detail : e.getClass().getSimpleName()));
            throw e;
        } finally {
            if (trans != null) {
                dataTransEngine.removeTrans(taskId);
            }

            logChannel.addLog(Constants.EXECUTE_STATUS.SUCCESS, "流程执行结束！");
            logChannel.setStatus(success ? Constants.EXECUTE_STATUS.SUCCESS : Constants.EXECUTE_STATUS.FAILURE);
            logChannel.setFinishDate(new Date());
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
     * 停止执行转换任务
     *
     * @param taskId 转换任务id
     */
    public void stopExecute(Integer taskId) {
        transTaskWorkExecutor.execute(() -> {
            TransTask current = transTaskMapper.selectById(taskId);
            String failurePolicy = current != null ? current.getFailurePolicy() : "1";
            try {
                stopExecuteBlocking(taskId);
                markTransTaskStop(taskId);
            } catch (Exception e) {
                log.error("停止执行转换任务失败, taskId={}", taskId, e);
                markTransTaskFailed(taskId, failurePolicy, e);
            }
        });
    }

    private static void registerRunningTrans(Integer taskId, String instanceId) {
        if (taskId != null && StringUtils.isNotBlank(instanceId)) {
            RUNNING_TASK_TO_INSTANCE.put(taskId, instanceId);
        }
    }

    private static void unregisterRunningTrans(Integer taskId, String instanceId) {
        if (taskId != null && StringUtils.isNotBlank(instanceId)) {
            RUNNING_TASK_TO_INSTANCE.remove(taskId, instanceId);
        }
    }

    /**
     * 单次运行的资源 id：taskId + runId，避免并发或连续触发时 LogChannel 互相覆盖。
     */
    private void stopExecuteBlocking(Integer taskId) {
        log.info("停止执行转换任务, id={}", taskId);

        if (getTransTaskById(taskId) == null) {
            log.warn("转换任务不存在, id={}", taskId);
            return;
        }

        boolean running = dataTransEngine.checkTransStatus(taskId) || RUNNING_TASK_TO_INSTANCE.containsKey(taskId);
        if (!running) {
            log.info("未发现运行中的 Kettle 转换实例（可能尚未启动或已结束）, taskId={}", taskId);
            return;
        }

        dataTransEngine.stopTrans(taskId);
        dataTransEngine.removeTrans(taskId);
        RUNNING_TASK_TO_INSTANCE.remove(taskId);
    }

    /**
     * 标记转换任务为已停止
     *
     * @param taskId 转换任务ID
     */
    private void markTransTaskStop(Integer taskId) {
        Date finishedAt = new Date();
        UpdateWrapper<TransTask> toInit = new UpdateWrapper<>();
        toInit.eq("id", taskId)
                .eq("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                .set("status", Constants.JOB_MANAGE_STATUS.STOP)
                .set("reason", "")
                .set("updated_time", finishedAt)
                .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
        transTaskMapper.update(null, toInit);
    }

    /**
     * 根据id查询转换任务
     *
     * @param taskId 转换任务id
     * @return 转换任务
     */
    private TransTask getTransTaskById(Integer taskId) {
        LambdaQueryWrapper<TransTask> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TransTask::getId, taskId)
                .eq(TransTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        return transTaskMapper.selectOne(queryWrapper);
    }

    /**
     * 根据id查询转换流
     *
     * @param flowId 转换流id
     * @return 转换流
     */
    private TransFlow getTransFlowById(Integer flowId) {
        LambdaQueryWrapper<TransFlow> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TransFlow::getId, flowId)
                .eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE);
        return transFlowMapper.selectOne(queryWrapper);
    }
}
