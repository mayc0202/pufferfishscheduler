package com.pufferfishscheduler.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 转换任务在 worker 内异步执行，避免长时间占用 Kafka 消费线程。
 */
@Configuration
public class TransTaskExecutorPoolConfig {

    @Bean(name = "transTaskWorkExecutor")
    public Executor transTaskWorkExecutor(
            @Value("${trans.task.pool.core-size:2}") int coreSize,
            @Value("${trans.task.pool.max-size:8}") int maxSize,
            @Value("${trans.task.pool.queue-capacity:100}") int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(coreSize);
        executor.setMaxPoolSize(maxSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("trans-task-work-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(120);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
