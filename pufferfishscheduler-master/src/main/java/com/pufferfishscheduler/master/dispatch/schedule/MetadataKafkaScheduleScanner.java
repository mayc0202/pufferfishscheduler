package com.pufferfishscheduler.master.dispatch.schedule;

import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.MetadataTask;
import com.pufferfishscheduler.dao.mapper.MetadataTaskMapper;
import com.pufferfishscheduler.master.dispatch.kafka.TaskDispatchKafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;

import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Master 侧：计算 cron 下一次触发时间，并把“调度消息”投递到 Kafka（方案B）
 */
@Component
public class MetadataKafkaScheduleScanner {

    private final MetadataTaskMapper metadataTaskMapper;
    private final TaskDispatchKafkaProducer producer;
    private final AtomicBoolean scanning = new AtomicBoolean(false);

    /**
     * 触发窗口（<=1分钟内）
     */
    @Value("${scheduler.metadata.trigger-window-ms:60000}")
    private long triggerWindowMs;

    /**
     * 任务卡住阈值（30分钟）
     */
    @Value("${scheduler.metadata.stuck-threshold-ms:300000}")
    private long stuckThresholdMs;

    public MetadataKafkaScheduleScanner(MetadataTaskMapper metadataTaskMapper,
                                          TaskDispatchKafkaProducer producer) {
        this.metadataTaskMapper = metadataTaskMapper;
        this.producer = producer;
    }

    /**
     * 扫描元数据任务
     */
    @Scheduled(fixedDelayString = "${scheduler.metadata.scan-interval-ms:10000}")
    public void scan() {
        try {
            // 单 master 情况下防重入（避免一次扫描没结束下次又进来）
            if (!scanning.compareAndSet(false, true)) {
                return;
            }

            Date now = new Date();
            LambdaQueryWrapper<MetadataTask> query = new LambdaQueryWrapper<>();
            query.eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                    .isNotNull(MetadataTask::getCron);

            List<MetadataTask> tasks = metadataTaskMapper.selectList(query);
            if (tasks == null || tasks.isEmpty()) {
                return;
            }

            for (MetadataTask task : tasks) {
                if (task == null) continue;
                if (task.getEnable() == null || !task.getEnable()) continue;
                if (task.getStatus() == null) continue;

                if (Constants.JOB_MANAGE_STATUS.STOP.equals(task.getStatus())
                        || Constants.JOB_MANAGE_STATUS.STOPPING.equals(task.getStatus())) {
                    continue;
                }

                boolean isInProgress = Constants.JOB_MANAGE_STATUS.STARTING.equals(task.getStatus())
                        || Constants.JOB_MANAGE_STATUS.RUNNING.equals(task.getStatus());
                if (isInProgress) {
                    // 正常执行中：在阈值内跳过，避免重复投递；超过阈值则允许走后续逻辑做异常回收
                    // execute_time 为空仍停留在 STARTING/RUNNING 时，不能 continue（否则会永久卡死）
                    if (task.getExecuteTime() != null) {
                        long diff = now.getTime() - task.getExecuteTime().getTime();
                        if (diff <= stuckThresholdMs) {
                            continue;
                        }
                    }
                }

                // failure_policy：0继续 1停止
                if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(task.getStatus())
                        && "1".equals(task.getFailurePolicy())) {
                    continue;
                }

                // CronExpression.next(参考点) 返回的是「严格晚于参考点」的下一次触发。
                // 若参考点用「当前时刻」，对新建 INIT（execute_time 为空）会得到「未来的下一次」，永远进不了触发窗口。
                // 无 execute_time 时回拨参考点：至少回拨「触发窗口」；并对天/周级 cron 保证足够长的回看（仅回拨 triggerWindow 时，
                // next(现在-1分钟) 对「每天 0 点」类表达式仍可能直接落到「明天 0 点」导致永远不触发）。
                Date reference;
                if (task.getExecuteTime() != null) {
                    reference = task.getExecuteTime();
                } else {
                    long minLookback = Math.max(triggerWindowMs + 1000L, 86_400_000L); // >= 1 天
                    long refMs = now.getTime() - minLookback;
                    reference = new Date(Math.max(0L, refMs));
                }
                CronExpression cronExpression = parseCron(task.getCron());
                if (cronExpression == null) continue;

                ZoneId zoneId = ZoneId.systemDefault();
                Date next;
                try {
                    next = Date.from(cronExpression.next(reference.toInstant().atZone(zoneId)).toInstant());
                } catch (Exception e) {
                    continue;
                }
                if (next == null) continue;

                long nextTimeMs = next.getTime();
                long nowMs = now.getTime();
                if (nextTimeMs > nowMs) continue;
                if (nowMs - nextTimeMs > triggerWindowMs) continue;

                Date scheduledDate = truncateToSecond(new Date(nextTimeMs));

                UpdateWrapper<MetadataTask> uw = new UpdateWrapper<>();
                uw.eq("id", task.getId())
                        .eq("deleted", Constants.DELETE_FLAG.FALSE)
                        .set("status", Constants.JOB_MANAGE_STATUS.STARTING)
                        .set("execute_time", scheduledDate)
                        // 新一次调度开始，清空上次失败原因（与实时任务“由执行结果写 reason”一致）
                        .set("reason", "")
                        .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT)
                        .set("updated_time", now);

                int updated = metadataTaskMapper.update(null, uw);
                if (updated != 1) {
                    continue;
                }

                TaskDispatchMessage msg = new TaskDispatchMessage();
                msg.setTaskType(Constants.TASK_TYPE.METADATA_TASK);
                msg.setTaskId(task.getId());
                msg.setDbId(task.getDbId());
                msg.setScheduledTimeMillis(scheduledDate.getTime());

                producer.send(msg);
            }
        } catch (Exception e) {
            // 调度侧失败不影响主流程
            e.printStackTrace();
        } finally {
            scanning.set(false);
        }
    }

    private static Date truncateToSecond(Date date) {
        long ms = date.getTime();
        return new Date((ms / 1000) * 1000);
    }

    private static CronExpression parseCron(String cron) {
        if (cron == null) return null;
        String s = cron.trim();
        // 兼容 5 字段 cron（补秒）
        if (s.split("\\s+").length == 5) {
            s = "0 " + s;
        }
        try {
            return CronExpression.parse(s);
        } catch (Exception e) {
            return null;
        }
    }
}

