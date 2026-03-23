package com.pufferfishscheduler.master.collect.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.cdc.kafka.config.AppProperties;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.RtTask;
import com.pufferfishscheduler.dao.mapper.RtTaskMapper;
import com.pufferfishscheduler.cdc.kafka.entity.RealTimeSyncTaskStatus;
import com.pufferfishscheduler.master.collect.realtime.service.RealTimeTaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实时数据同步任务作业
 *
 * @author Mayc
 * @since 2026-03-17  15:41
 */
@Slf4j
@Component
public class RealTimeSyncTaskJob {

    @Autowired
    private AppProperties appProperties;

    @Autowired
    private RealTimeTaskService realTimeTaskService;

    @Autowired
    private RtTaskMapper rtTaskMapper;

    /**
     * 实时数据同步任务发送次数映射表
     */
    private Map<Integer, Integer> sentCountMap = new ConcurrentHashMap<>();

    /**
     * 执行实时数据同步任务
     * 每10秒执行一次
     */
    @Scheduled(cron = "0/10 * * * * ?")
    public void execute() {

        // 开发模式下不执行
        if (appProperties.getDevModeEnabled()) {
            return;
        }

        try {
            log.debug("开始实时同步任务实时监控...");
            // 查询启动后未停止的任务列表: 2 4 1 5
            List<String> statusList = buildUnStopTaskStatusList();
            List<RtTask> taskList = realTimeTaskService.getTaskListByStatus(statusList);
            if (CollectionUtils.isEmpty(taskList)) {
                log.debug("没有发现正在运行的实时任务...");
                return;
            }

            // TODO 初始化发送次数映射表
            if (sentCountMap.isEmpty()) {
                initSentCountMap();
            }

            /**
             * 1. 启停实时任务。如果任务是启动中的，那么需要调用引擎启动任务。如果认识停止中的，那么需要调用引擎来停止任务。
             */
            log.debug("开始执行实时数据同步任务调度...");
            scheduleTask(taskList);
            log.debug("完成实时数据同步任务调度。");

            // scheduleTask 内 start/stop 会落库，但 taskList 仍是本轮查询时的内存对象；必须与 DB 对齐，避免用陈旧 STARTING 误判 Connect 状态并覆盖 FAILURE
            syncTaskListStateFromDb(taskList);

            /**
             * 2. 监控任务运行状态。异常任务发送预警消息
             */
            log.debug("开始更新任务状态...");

            for (RtTask rtTask : taskList) {

                try {
                    if (Constants.JOB_MANAGE_STATUS.STARTING.equals(rtTask.getStatus())
                            || Constants.JOB_MANAGE_STATUS.STOPPING.equals(rtTask.getStatus())) {
                        // TODO 统计发送邮件计数器归零
                        sentCountMap.put(rtTask.getId(), 0);
                        // STARTING/STOPPING 阶段也需要监控真实状态：
                        // 一旦连接器已经 RUNNING/STOP/FAILURE/UNASSIGNED，应立即刷新任务状态，避免长期卡在 STARTING/STOPPING。
                        RealTimeSyncTaskStatus realStatus = realTimeTaskService.getTaskRealStatus(rtTask.getId());
                        if (realStatus != null) {
                            refreshTaskStatus(rtTask, realStatus);
                        }
                        continue;
                    }

                    // 获取任务真实状态
                    RealTimeSyncTaskStatus realStatus = realTimeTaskService.getTaskRealStatus(rtTask.getId());

                    /**
                     * 2.1 刷新任务状态
                     */
                    refreshTaskStatus(rtTask, realStatus);

                } catch (Exception e) {
                    log.error("任务ID:" + rtTask.getId(), e);
                }

            }

            log.debug("完成任务状态更新。");
            log.debug("结束更新实时任务。");
        } catch (Exception e) {
            log.error("执行实时数据同步任务失败", e);
        }
    }

    /**
     * 用数据库最新状态覆盖本轮 taskList 中的内存字段，避免 start() 已写 FAILURE 后仍带着 STARTING 做 refresh。
     */
    private void syncTaskListStateFromDb(List<RtTask> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        for (RtTask rtTask : taskList) {
            if (rtTask == null || rtTask.getId() == null) {
                continue;
            }
            LambdaQueryWrapper<RtTask> w = new LambdaQueryWrapper<>();
            w.eq(RtTask::getId, rtTask.getId()).eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE);
            RtTask fresh = rtTaskMapper.selectOne(w);
            if (fresh != null) {
                rtTask.setStatus(fresh.getStatus());
                rtTask.setReason(fresh.getReason());
                rtTask.setRuntimeConfig(fresh.getRuntimeConfig());
            }
        }
    }

    /**
     * 调度实时数据同步任务
     *
     * @param task
     */
    private void scheduleTask(List<RtTask> task) {
        for (RtTask rtTask : task) {
            try {
                // 情况 A：发现是 STARTING，调用 start() 执行复杂的 Kafka 创建逻辑
                if (Constants.JOB_MANAGE_STATUS.STARTING.equals(rtTask.getStatus())) {
                    log.info("检测到任务 {} 处于 STARTING，开始异步拉起...", rtTask.getId());
                    realTimeTaskService.start(rtTask.getId());
                }
                // 情况 B：发现是 STOPPING，调用 stop() 执行清理逻辑
                else if (rtTask.getStatus().equals(Constants.JOB_MANAGE_STATUS.STOPPING)) {
                    log.info("检测到任务 {} 处于 STOPPING，开始异步停止...", rtTask.getId());
                    realTimeTaskService.stop(rtTask.getId());
                }
            } catch (Exception e) {
                String errMsg = "调度实时数据同步任务失败，任务ID：" + rtTask.getId() + "，原因：" + e.getMessage();
                log.error(errMsg, e);
                // 定时任务路径下，所有异常都写入任务 reason，不抛业务异常
                realTimeTaskService.markFailedFromScheduler(rtTask.getId(), errMsg);
                // 同步内存状态，避免同一轮调度后续逻辑仍使用旧状态（如 STARTING）
                rtTask.setStatus(Constants.JOB_MANAGE_STATUS.FAILURE);
                rtTask.setReason(errMsg);
            }
        }
    }

    /**
     * 更新实时数据同步任务状态
     *
     * @param rtTask
     * @param realStatus
     */
    public void refreshTaskStatus(RtTask rtTask, RealTimeSyncTaskStatus realStatus) {
        if (rtTask == null || realStatus == null) {
            return;
        }

        // 以库表为准，避免内存对象仍是 STARTING 时误判
        LambdaQueryWrapper<RtTask> w = new LambdaQueryWrapper<>();
        w.eq(RtTask::getId, rtTask.getId()).eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE);
        RtTask dbTask = rtTaskMapper.selectOne(w);
        String dbStatus = dbTask != null ? dbTask.getStatus() : rtTask.getStatus();
        String dbReason = dbTask != null ? dbTask.getReason() : rtTask.getReason();

        String real = realStatus.getStatus();
        String msg = realStatus.getMessage();

        // STOPPING 阶段下，连接器被 stop() 删除后通常会表现为 UNASSIGNED/404。
        // 这属于停止完成，不应误刷为 FAILED。
        if (Constants.JOB_MANAGE_STATUS.STOPPING.equals(dbStatus)
                && RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED.equals(real)) {
            realTimeTaskService.updateTaskStatus(rtTask.getId(),
                    Constants.JOB_MANAGE_STATUS.STOP,
                    "",
                    Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                    "");
            return;
        }

        // RUNNING -> RUNNING（已失败的任务不要被 Connect 瞬时 RUNNING 覆盖）
        if (RealTimeSyncTaskStatus.TASK_STATUS_RUNNING.equals(real)) {
            if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(dbStatus)) {
                return;
            }
            if (!Constants.JOB_MANAGE_STATUS.RUNNING.equals(dbStatus)) {
                realTimeTaskService.updateTaskStatus(rtTask.getId(),
                        Constants.JOB_MANAGE_STATUS.RUNNING,
                        "",
                        Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                        "");
            }
            return;
        }

        // STOP -> STOP（停止是正常状态，不应刷成 FAILED）
        if (RealTimeSyncTaskStatus.TASK_STATUS_STOP.equals(real)) {
            if (!Constants.JOB_MANAGE_STATUS.STOP.equals(dbStatus)) {
                realTimeTaskService.updateTaskStatus(rtTask.getId(),
                        Constants.JOB_MANAGE_STATUS.STOP,
                        "",
                        Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                        "");
            }
            return;
        }

        // UNASSIGNED / FAILURE -> FAILED（无论当前是 STARTING/STOPPING，都要落库 reason）
        if (RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED.equals(real)
                || RealTimeSyncTaskStatus.TASK_STATUS_FAILURE.equals(real)) {
            String reason = (msg == null || msg.isBlank()) ? "连接器未分配或运行异常" : msg;
            // 只要状态不一致（或 reason 为空），就强制落到 FAILED 并写 reason
            if (!Constants.JOB_MANAGE_STATUS.FAILURE.equals(dbStatus)
                    || dbReason == null
                    || dbReason.isBlank()) {
                realTimeTaskService.updateTaskStatus(rtTask.getId(),
                        Constants.JOB_MANAGE_STATUS.FAILURE,
                        reason,
                        Constants.SYS_OP_INFO.SYSTEM_ACCOUNT,
                        "");
            }
        }
    }

    /**
     * 构建启动后未停止的任务状态列表
     *
     * @return 任务状态列表
     */
    private List<String> buildUnStopTaskStatusList() {
        return List.of(
                Constants.JOB_MANAGE_STATUS.RUNNING,
                Constants.JOB_MANAGE_STATUS.FAILURE,
                Constants.JOB_MANAGE_STATUS.STARTING,
                Constants.JOB_MANAGE_STATUS.STOPPING);
    }

    private void initSentCountMap() {
        String dateFormat = dateFormat(new Date());
        dataConverter(dateFormat, 0);
        dataConverter(dateFormat, 1);
        // TODO 查询指定日期范围内的任务告警信息发送次数
//        List<MonitorNumVo> monitorNumVos = monitorMessageDao.countRtTaskNum(startDate, endDate);
//        if (CollectionUtils.isNotEmpty(monitorNumVos)) {
//            sentCountMap = monitorNumVos.stream().collect(Collectors.toMap(MonitorNumVo::getId, MonitorNumVo::getNum));
//        }
    }

    /***
     * 将Date类转换成指定格式
     * @param nowDate
     * @return
     */
    public String dateFormat(Date nowDate) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(nowDate);
    }

    /**
     * 日期数据转换
     *
     * @param date
     * @param type
     * @return
     */
    private Date dataConverter(String date, int type) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            if (type == 0) {
                return sdf.parse(date + " 00:00:00");
            } else if (type == 1) {
                return sdf.parse(date + " 23:59:59");
            }
        } catch (ParseException e) {
            return null;
        }
        return null;
    }
}
