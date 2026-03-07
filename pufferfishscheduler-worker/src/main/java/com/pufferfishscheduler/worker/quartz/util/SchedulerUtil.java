package com.pufferfishscheduler.worker.quartz.util;

import com.pufferfishscheduler.domain.model.quartz.QuartzJobDetail;
import com.pufferfishscheduler.worker.quartz.constants.WorkerConstants;
import com.pufferfishscheduler.worker.quartz.job.QuartzDisallowConcurrentExecution;
import com.pufferfishscheduler.worker.quartz.job.QuartzJobExecution;
import org.quartz.*;

/**
 * @author Mayc
 * @since 2025-07-30  13:03
 */
public class SchedulerUtil {
    /**
     * 得到quartz任务类
     *
     * @param job 执行计划
     * @return 具体执行任务类
     */
    private static Class<? extends Job> getQuartzJobClass(QuartzJobDetail job) {
        boolean isConcurrent = "0".equals(job.getConcurrent());
        return isConcurrent ? QuartzJobExecution.class : QuartzDisallowConcurrentExecution.class;
    }

    /**
     * 构建任务触发对象
     */
    public static TriggerKey getTriggerKey(Long jobId, String jobGroup) {
        return TriggerKey.triggerKey(WorkerConstants.TASK_CLASS_NAME + jobId, jobGroup);
    }

    /**
     * 构建任务键对象
     */
    public static JobKey getJobKey(Long jobId, String jobGroup) {
        return JobKey.jobKey(WorkerConstants.TASK_CLASS_NAME + jobId, jobGroup);
    }

    /**
     * 创建定时任务
     */
    public static void createScheduleJob(Scheduler scheduler, QuartzJobDetail job) throws Exception {
        Class<? extends Job> jobClass = getQuartzJobClass(job);
        // 构建job信息
        Long jobId = job.getJobId();
        String jobGroup = job.getJobGroup();
        JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(getJobKey(jobId, jobGroup)).build();

        // 表达式调度构建器
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCronExpression());
        cronScheduleBuilder = handleCronScheduleMisfirePolicy(job, cronScheduleBuilder);

        // 按新的cronExpression表达式构建一个新的trigger
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(getTriggerKey(jobId, jobGroup))
                .withSchedule(cronScheduleBuilder).build();

        // 放入参数，运行时的方法可以获取
        jobDetail.getJobDataMap().put(WorkerConstants.TASK_PROPERTIES, job);

        // 判断是否存在
        if (scheduler.checkExists(getJobKey(jobId, jobGroup))) {
            // 防止创建时存在数据问题 先移除，然后在执行创建操作
            scheduler.deleteJob(getJobKey(jobId, jobGroup));
        }

        scheduler.scheduleJob(jobDetail, trigger);

        // 暂停任务
        if (job.getStatus().equals(WorkerConstants.Status.PAUSE.getValue())) {
            scheduler.pauseJob(SchedulerUtil.getJobKey(jobId, jobGroup));
        }
    }

    /**
     * 设置定时任务策略
     */
    public static CronScheduleBuilder handleCronScheduleMisfirePolicy(QuartzJobDetail job, CronScheduleBuilder cb)
            throws Exception {
        switch (job.getMisfirePolicy()) {
            case WorkerConstants.MISFIRE_DEFAULT:
                return cb;
            // 立即补偿所有错过执行           关键实时任务
            case WorkerConstants.MISFIRE_IGNORE_MISFIRES:
                return cb.withMisfireHandlingInstructionIgnoreMisfires();
            // 立即执行一次并继续原调度        常规任务
            case WorkerConstants.MISFIRE_FIRE_AND_PROCEED:
                return cb.withMisfireHandlingInstructionFireAndProceed();
            // 跳过错过执行等待下次触发        非重要任务
            case WorkerConstants.MISFIRE_DO_NOTHING:
                return cb.withMisfireHandlingInstructionDoNothing();
            default:
                throw new Exception("The task misfire policy '" + job.getMisfirePolicy()
                        + "' cannot be used in cron schedule tasks");
        }
    }
}
