package com.pufferfishscheduler.worker.quartz.job;

import com.pufferfishscheduler.domain.model.QuartzJobDetail;
import com.pufferfishscheduler.worker.quartz.util.JobInvokeUtil;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.PersistJobDataAfterExecution;

/**
 * 禁止并发执行的任务实现
 *
 * @author Mayc
 * @description
 * @since 2025-07-30  13:09
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class QuartzDisallowConcurrentExecution extends AbstractQuartzJob {
    @Override
    protected void doExecute(JobExecutionContext context, QuartzJobDetail job) throws Exception {
        JobInvokeUtil.invokeMethod(job);
    }
}