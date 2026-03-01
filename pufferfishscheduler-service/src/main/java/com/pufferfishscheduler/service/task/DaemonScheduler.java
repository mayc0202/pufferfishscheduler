package com.pufferfishscheduler.service.task;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 调度器
 *
 * @author Mayc
 * @since 2025-12-02  13:49
 */
@Slf4j
@Component
public class DaemonScheduler {


    /**
     * 每分钟执行任务状态同步操作
     */
    @Scheduled(cron = "0 0/1 * * * ?")
    private void scheduler() {
        // 1.先查询定时任务


        // 2.计算定时时间差

        // 3.修改定时任务状态


    }
}
