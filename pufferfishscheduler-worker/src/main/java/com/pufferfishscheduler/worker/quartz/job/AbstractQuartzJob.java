package com.pufferfishscheduler.worker.quartz.job;

import com.pufferfishscheduler.domain.model.QuartzJobDetail;
import com.pufferfishscheduler.worker.quartz.constants.WorkerConstants;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.util.Date;

/**
 * @author Mayc
 * @since 2025-07-30  13:06
 */
public abstract class AbstractQuartzJob implements Job {
    private static final Logger log = LoggerFactory.getLogger(AbstractQuartzJob.class);

    /**
     * 线程本地变量
     */
    private static final ThreadLocal<Date> THREAD_LOCAL = new ThreadLocal<>();

    @Override
    public void execute(JobExecutionContext context) {
        QuartzJobDetail job = new QuartzJobDetail();
        BeanUtils.copyProperties(job, context.getMergedJobDataMap().get(WorkerConstants.TASK_PROPERTIES));
        try {
            // 执行前
            before(context, job);
            if (job != null) {
                doExecute(context, job);
            }
            // 执行后
            after(context, job, null);
        } catch (Exception e) {
            log.error("任务执行异常  - ：", e);
            after(context, job, e);
        }
    }

    /**
     * 执行前
     *
     * @param context 工作执行上下文对象
     * @param job     系统计划任务
     */
    protected void before(JobExecutionContext context, QuartzJobDetail job) {
        THREAD_LOCAL.set(new Date());
    }

    /**
     * 执行后
     *
     * @param context 工作执行上下文对象
     * @param sysJob  系统计划任务
     */
    protected void after(JobExecutionContext context, QuartzJobDetail sysJob, Exception e) {

    }

    /**
     * 执行方法，由子类重载
     *
     * @param context 工作执行上下文对象
     * @param job     系统计划任务
     * @throws Exception 执行过程中的异常
     */
    protected abstract void doExecute(JobExecutionContext context, QuartzJobDetail job) throws Exception;
}