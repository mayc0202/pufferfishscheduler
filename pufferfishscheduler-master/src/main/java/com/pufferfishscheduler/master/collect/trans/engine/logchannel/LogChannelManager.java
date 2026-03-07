package com.pufferfishscheduler.master.collect.trans.engine.logchannel;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.pufferfishscheduler.common.constants.Constants;

import lombok.extern.slf4j.Slf4j;

/**
 * 日志通道管理器
 * 
 * @author Mayc
 *
 */
@Slf4j
public class LogChannelManager {
    // 日志通道映射
    private static final Map<String, LogChannel> logs = new ConcurrentHashMap<>();
    
    // 配置参数
    private static final long LOG_RETENTION_MINUTES = 5; // 日志保留时间（分钟）
    private static final long CLEANUP_INTERVAL_SECONDS = 10; // 清理间隔（秒）
    private static final int MAX_LOG_CHANNELS = 1000; // 最大日志通道数量
    
    // 定时执行器
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "LogChannelCleanupThread");
        thread.setDaemon(true);
        return thread;
    });
    
    static {
        // 初始化清理任务
        initCleanupTask();
    }
    
    /**
     * 初始化清理任务
     */
    private static void initCleanupTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredLogs();
            } catch (Exception e) {
                log.error("Error during log cleanup", e);
            }
        }, CLEANUP_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * 清理过期日志
     */
    private static void cleanupExpiredLogs() {
        Iterator<Map.Entry<String, LogChannel>> iterator = logs.entrySet().iterator();
        long currentTime = System.currentTimeMillis();
        long retentionMillis = LOG_RETENTION_MINUTES * 60 * 1000;
        
        while (iterator.hasNext()) {
            Map.Entry<String, LogChannel> entry = iterator.next();
            LogChannel logChannel = entry.getValue();
            
            if (isLogExpired(logChannel, currentTime, retentionMillis)) {
                iterator.remove();
                log.debug("移除失效的日志信息。标识符ID为：{}，当前日志缓存数量：{}", 
                         entry.getKey(), logs.size());
            }
        }
        
        // 检查是否超过最大通道数量
        checkMaxChannels();
    }
    
    /**
     * 检查日志是否过期
     * @param logChannel 日志通道
     * @param currentTime 当前时间戳
     * @param retentionMillis 保留时间（毫秒）
     * @return 是否过期
     */
    private static boolean isLogExpired(LogChannel logChannel, long currentTime, long retentionMillis) {
        // 检查状态是否为完成状态
        if (Constants.EXECUTE_STATUS.SUCCESS.equals(logChannel.getStatus()) 
                || Constants.EXECUTE_STATUS.FAILURE.equals(logChannel.getStatus())) {
            
            // 如果没有完成时间，设置为当前时间
            if (logChannel.getFinishDate() == null) {
                logChannel.setFinishDate(new Date());
                return false;
            }
            
            // 检查是否超过保留时间
            long specifiedTimeMillis = logChannel.getFinishDate().getTime() + retentionMillis;
            return specifiedTimeMillis < currentTime;
        }
        return false;
    }
    
    /**
     * 检查是否超过最大通道数量
     */
    private static void checkMaxChannels() {
        if (logs.size() > MAX_LOG_CHANNELS) {
            log.warn("日志通道数量超过上限：{}，当前数量：{}", MAX_LOG_CHANNELS, logs.size());
            
            // 可以在这里添加额外的清理逻辑，例如移除最早的日志通道
        }
    }
    
    /**
     * 添加日志通道
     * @param key 键
     * @param logChannel 日志通道
     */
    public static void put(String key, LogChannel logChannel) {
        if (StringUtils.isNotEmpty(key) && logChannel != null) {
            logs.put(key, logChannel);
            log.debug("添加日志通道。键：{}，当前日志缓存数量：{}", key, logs.size());
        }
    }
    
    /**
     * 移除日志通道
     * @param key 键
     */
    public static void remove(String key) {
        if (StringUtils.isNotEmpty(key)) {
            logs.remove(key);
            log.debug("移除日志通道。键：{}，当前日志缓存数量：{}", key, logs.size());
        }
    }
    
    /**
     * 获取日志通道
     * @param key 键
     * @return 日志通道
     */
    public static LogChannel get(String key) {
        return logs.get(key);
    }
    
    /**
     * 生成日志通道键
     * @param resourceType 资源类型
     * @param id 资源ID
     * @return 日志通道键
     */
    public static String getKey(String resourceType, String id) {
        if (StringUtils.isBlank(resourceType) || StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("资源类型和ID不能为空");
        }
        return String.format("%s-%s", resourceType, id);
    }
    
    /**
     * 获取当前日志通道数量
     * @return 日志通道数量
     */
    public static int getLogChannelCount() {
        return logs.size();
    }
    
    /**
     * 关闭管理器
     */
    public static void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logs.clear();
        log.info("日志通道管理器已关闭");
    }
}
