package com.pufferfishscheduler.worker.quartz.job;

import com.pufferfishscheduler.domain.model.quartz.QuartzJobDetail;
import com.pufferfishscheduler.worker.quartz.util.JobInvokeUtil;
import org.quartz.JobExecutionContext;

/**
 * 并发执行的任务实现
 *
 * @author Mayc
 * @since 2025-07-30  13:08
 */
public class QuartzJobExecution extends AbstractQuartzJob {
    @Override
    protected void doExecute(JobExecutionContext context, QuartzJobDetail job) throws Exception {
        JobInvokeUtil.invokeMethod(job);
    }
}
