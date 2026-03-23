package com.pufferfishscheduler.master.dispatch.schedule;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.dao.mapper.TransTaskMapper;
import com.pufferfishscheduler.master.dispatch.kafka.TaskDispatchKafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Master：转换任务 cron 扫描，投递 taskType=trans_task 到 Kafka（与元数据任务扫描器逻辑对齐）
 */
@Component
public class TransKafkaScheduleScanner {

    private final TransTaskMapper transTaskMapper;
    private final TaskDispatchKafkaProducer producer;
    private final AtomicBoolean scanning = new AtomicBoolean(false);

    @Value("${scheduler.trans.trigger-window-ms:60000}")
    private long triggerWindowMs;

    @Value("${scheduler.trans.stuck-threshold-ms:300000}")
    private long stuckThresholdMs;

    public TransKafkaScheduleScanner(TransTaskMapper transTaskMapper,
                                     TaskDispatchKafkaProducer producer) {
        this.transTaskMapper = transTaskMapper;
        this.producer = producer;
    }

    @Scheduled(fixedDelayString = "${scheduler.trans.scan-interval-ms:10000}")
    public void scan() {
        try {
            if (!scanning.compareAndSet(false, true)) {
                return;
            }

            Date now = new Date();
            LambdaQueryWrapper<TransTask> query = new LambdaQueryWrapper<>();
            query.eq(TransTask::getDeleted, Constants.DELETE_FLAG.FALSE)
                    .isNotNull(TransTask::getCron);

            List<TransTask> tasks = transTaskMapper.selectList(query);
            if (tasks == null || tasks.isEmpty()) {
                return;
            }

            for (TransTask task : tasks) {
                if (task == null) {
                    continue;
                }
                if (task.getEnable() == null || !task.getEnable()) {
                    continue;
                }
                if (task.getStatus() == null) {
                    continue;
                }

                if (Constants.JOB_MANAGE_STATUS.STOP.equals(task.getStatus())
                        || Constants.JOB_MANAGE_STATUS.STOPPING.equals(task.getStatus())) {
                    continue;
                }

                boolean isInProgress = Constants.JOB_MANAGE_STATUS.STARTING.equals(task.getStatus())
                        || Constants.JOB_MANAGE_STATUS.RUNNING.equals(task.getStatus());
                if (isInProgress) {
                    if (task.getExecuteTime() != null) {
                        long diff = now.getTime() - task.getExecuteTime().getTime();
                        if (diff <= stuckThresholdMs) {
                            continue;
                        }
                    }
                }

                if (Constants.JOB_MANAGE_STATUS.FAILURE.equals(task.getStatus())
                        && "1".equals(task.getFailurePolicy())) {
                    continue;
                }

                Date reference;
                if (task.getExecuteTime() != null) {
                    reference = task.getExecuteTime();
                } else {
                    long minLookback = Math.max(triggerWindowMs + 1000L, 86_400_000L);
                    long refMs = now.getTime() - minLookback;
                    reference = new Date(Math.max(0L, refMs));
                }
                CronExpression cronExpression = parseCron(task.getCron());
                if (cronExpression == null) {
                    continue;
                }

                ZoneId zoneId = ZoneId.systemDefault();
                Date next;
                try {
                    next = Date.from(cronExpression.next(reference.toInstant().atZone(zoneId)).toInstant());
                } catch (Exception e) {
                    continue;
                }
                if (next == null) {
                    continue;
                }

                long nextTimeMs = next.getTime();
                long nowMs = now.getTime();
                if (nextTimeMs > nowMs) {
                    continue;
                }
                if (nowMs - nextTimeMs > triggerWindowMs) {
                    continue;
                }

                Date scheduledDate = truncateToSecond(new Date(nextTimeMs));

                UpdateWrapper<TransTask> uw = new UpdateWrapper<>();
                uw.eq("id", task.getId())
                        .eq("deleted", Constants.DELETE_FLAG.FALSE)
                        .set("status", Constants.JOB_MANAGE_STATUS.STARTING)
                        .set("execute_time", scheduledDate)
                        .set("reason", "")
                        .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT)
                        .set("updated_time", now);

                int updated = transTaskMapper.update(null, uw);
                if (updated != 1) {
                    continue;
                }

                TaskDispatchMessage msg = new TaskDispatchMessage();
                msg.setTaskType(Constants.TASK_TYPE.TRANS_TASK);
                msg.setTaskId(task.getId());
                msg.setScheduledTimeMillis(scheduledDate.getTime());

                producer.send(msg);
            }
        } catch (Exception e) {
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
        if (cron == null) {
            return null;
        }
        String s = cron.trim();
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
