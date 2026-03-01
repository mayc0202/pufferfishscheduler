package com.pufferfishscheduler.worker.service;

import com.pufferfishscheduler.api.worker.SchedulerManagerServer;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.QuartzJob;
import com.pufferfishscheduler.dao.mapper.QuartzJobMapper;
import com.pufferfishscheduler.domain.model.QuartzJobDetail;
import com.pufferfishscheduler.worker.quartz.util.SchedulerUtil;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Scheduler 调度器服务实现
 *
 * @author Mayc
 * @since 2025-07-30  13:25
 */
@Slf4j
//@DubboService(
//        version = "1.0.0",        // 服务版本（灰度发布/兼容性用）
//        group = "worker",         // 服务分组（区分不同环境/业务）
//        timeout = 3000,           // 调用超时（单位：ms，根据任务创建耗时调整）
//        retries = 0               // 重试次数（创建/删除任务为非幂等操作，禁止重试）
//)
@Service
public class SchedulerManager implements SchedulerManagerServer {

    private Scheduler scheduler;
    private final ReentrantLock lock = new ReentrantLock();

    @Value("${quartz.config.location:quartz.properties}")
    private String quartzConfigPath;

    @Autowired
    private QuartzJobMapper quartzJobMapper;

    /**
     * 初始化任务调度容器
     */
    @PostConstruct
    public void init() {
        lock.lock();
        try {
            if (scheduler != null && scheduler.isStarted()) {
                log.warn("Scheduler already started");
                return;
            }

            // 从类路径加载配置文件
            Properties props = loadQuartzProperties();

            // 创建调度器实例
            StdSchedulerFactory factory = new StdSchedulerFactory(props);
            scheduler = factory.getScheduler();

            // 初始化定时任务
            initQuartzJobs();

            // 启动调度器
            scheduler.start();
            log.info("Quartz Scheduler started successfully");

        } catch (SchedulerException | IOException e) {
            log.error("Failed to initialize Quartz Scheduler", e);
            // 更优雅的处理方式
            if (scheduler != null) {
                try {
                    scheduler.shutdown();
                } catch (SchedulerException ex) {
                    log.error("Error during scheduler shutdown", ex);
                }
            }
            throw new BusinessException("Quartz initialization failed:" + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 初始化定时任务
     */
    private void initQuartzJobs() {
        try {
            // 清空所有
            scheduler.clear();

            // 查询处所有未删除的任务
            List<QuartzJob> jobList = quartzJobMapper.selectList(null);
            for (QuartzJob job : jobList) {
                // 构建定时任务对象
                QuartzJobDetail quartzJob = buildQuartzJobDetail(job);
                createJob(quartzJob);
            }
        } catch (Exception e) {
            log.error("Failed to initialize Quartz Jobs", e);
            throw new BusinessException(e);
        }
    }

    /**
     * 构建QuartzJobDetail对象
     *
     * @param job
     * @return
     */
    public QuartzJobDetail buildQuartzJobDetail(QuartzJob job) {
        QuartzJobDetail quartzJob = new QuartzJobDetail();
        quartzJob.setJobId(job.getJobId());
        quartzJob.setJobName(quartzJob.getJobName());
        quartzJob.setJobGroup(quartzJob.getJobGroup());
        quartzJob.setInvokeTarget(job.getInvokeTarget());
        quartzJob.setCronExpression(job.getCronExpression());
        quartzJob.setMisfirePolicy(job.getMisfirePolicy());
        quartzJob.setConcurrent(job.getConcurrent()); // 不允许并发执行
        quartzJob.setStatus(job.getStatus());
        return quartzJob;
    }

    /**
     * 加载定时任务配置
     *
     * @return
     * @throws IOException
     */
    private Properties loadQuartzProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream is = new ClassPathResource(quartzConfigPath).getInputStream()) {
            props.load(is);
            log.debug("Loaded Quartz config from: {}", quartzConfigPath);
            return props;
        } catch (IOException e) {
            log.error("Failed to load Quartz config at: {}", quartzConfigPath, e);
            throw e;
        }
    }

    /**
     * 应用销毁时
     */
    @PreDestroy
    public void destroy() {
        lock.lock();
        try {
            if (scheduler != null && scheduler.isStarted()) {
                // true: 等待所有任务完成
                scheduler.shutdown(true);
                log.info("Quartz Scheduler shutdown completed");
            }
        } catch (SchedulerException e) {
            log.error("Error during scheduler shutdown", e);
        } finally {
            lock.unlock();
        }
    }


    /**
     * 创建任务
     */
    @Override
    public void createJob(QuartzJobDetail job) throws Exception {
        validateSchedulerState();
        SchedulerUtil.createScheduleJob(scheduler, job);
    }

    /**
     * 删除任务
     */
    @Override
    public void deleteJob(Long jobId, String jobGroup) throws Exception {
        validateSchedulerState();
        JobKey jobKey = SchedulerUtil.getJobKey(jobId, jobGroup);
        scheduler.deleteJob(jobKey);
    }

    /**
     * 暂停任务
     */
    @Override
    public void pauseJob(Long jobId, String jobGroup) throws Exception {
        validateSchedulerState();
        JobKey jobKey = SchedulerUtil.getJobKey(jobId, jobGroup);
        scheduler.pauseJob(jobKey);
    }

    /**
     * 恢复任务
     */
    @Override
    public void resumeJob(Long jobId, String jobGroup) throws Exception {
        validateSchedulerState();
        JobKey jobKey = SchedulerUtil.getJobKey(jobId, jobGroup);
        scheduler.resumeJob(jobKey);
    }

    /**
     * 更新任务
     */
    @Override
    public void updateJob(QuartzJobDetail job) throws Exception {
        validateSchedulerState();

        // 删除原任务
        deleteJob(job.getJobId(), job.getJobGroup());

        JobKey jobKey = SchedulerUtil.getJobKey(job.getJobId(), job.getJobGroup());
        // 移除旧触发器
        for (Trigger trigger : scheduler.getTriggersOfJob(jobKey)) {
            scheduler.unscheduleJob(trigger.getKey());
        }

        // 创建新任务
        createJob(job);
    }

    /**
     * 校验调度器状态
     */
    private void validateSchedulerState() {
        try {
            if (scheduler == null || !scheduler.isStarted()) {
                log.warn("Scheduler is not initialized or not started");
                throw new IllegalStateException("Scheduler is not initialized or not started");
            }
        } catch (SchedulerException e) {
            throw new BusinessException(e);
        }
    }
}
