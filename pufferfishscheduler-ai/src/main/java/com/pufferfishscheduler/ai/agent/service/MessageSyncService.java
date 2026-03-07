package com.pufferfishscheduler.ai.agent.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.ai.mannger.ChatMessageRedisManager;
import com.pufferfishscheduler.dao.entity.ChatMessageEntity;
import com.pufferfishscheduler.dao.entity.ChatSessionEntity;
import com.pufferfishscheduler.dao.mapper.ChatMessageMapper;
import com.pufferfishscheduler.dao.mapper.ChatSessionMapper;
import com.pufferfishscheduler.domain.model.agent.RedisMessageEntity;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis到数据库的消息同步服务
 * 定时将Redis中的消息批量同步到数据库
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageSyncService {

    private final ChatMessageRedisManager redisMessageManager;
    private final ChatMessageMapper chatMessageMapper;
    private final ChatSessionMapper chatSessionMapper;

    @Value("${message.sync.batch-size:100}")
    private int batchSize;

    @Value("${message.sync.cron:0/5 * * * * *}")
    private String syncCron;

    private final AtomicInteger syncCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);

    /**
     * 定时同步消息到数据库
     * 默认每5秒执行一次
     */
    @Scheduled(cron = "${message.sync.cron:0/5 * * * * *}")
    public void syncMessagesToDatabase() {
        long startTime = System.currentTimeMillis();

        try {
            // 获取待同步的消息
            List<RedisMessageEntity> pendingMessages = redisMessageManager.getPendingMessages(batchSize);

            if (pendingMessages.isEmpty()) {
                return;
            }

            log.debug("开始同步消息到数据库，数量：{}", pendingMessages.size());

            // 批量保存到数据库
            int successCount = batchSaveToDatabase(pendingMessages);

            // 统计
            int currentSyncCount = syncCount.addAndGet(successCount);
            int currentFailCount = failCount.addAndGet(pendingMessages.size() - successCount);

            long costTime = System.currentTimeMillis() - startTime;

            log.info("消息同步完成 - 成功：{}，失败：{}，总成功：{}，总失败：{}，耗时：{}ms",
                    successCount, pendingMessages.size() - successCount,
                    currentSyncCount, currentFailCount, costTime);

            // 如果待同步消息数量还很多，记录警告
            long remainingCount = redisMessageManager.getPendingMessageCount();
            if (remainingCount > batchSize * 10) {
                log.warn("待同步消息积压严重，当前待同步数量：{}", remainingCount);
            }

        } catch (Exception e) {
            log.error("消息同步过程中发生错误", e);
        }
    }

    /**
     * 批量保存到数据库
     */
    @Transactional(rollbackFor = Exception.class)
    public int batchSaveToDatabase(List<RedisMessageEntity> redisMessages) {
        if (redisMessages == null || redisMessages.isEmpty()) {
            return 0;
        }

        List<ChatMessageEntity> messageEntities = new ArrayList<>();
        int successCount = 0;

        try {
            for (RedisMessageEntity redisMessage : redisMessages) {
                try {
                    // 转换为数据库实体
                    ChatMessageEntity entity = convertToEntity(redisMessage);
                    messageEntities.add(entity);

                    // 更新会话统计
                    updateSessionStats(redisMessage.getSessionId(), redisMessage);

                    successCount++;
                } catch (Exception e) {
                    log.error("转换消息实体失败：{}", redisMessage.getMessageId(), e);
                    // 标记为同步失败，后续重试
                    redisMessageManager.markSyncFailed(redisMessage);
                }
            }

            // 批量插入消息
            if (!messageEntities.isEmpty()) {
                // 使用MyBatis-Plus的批量插入（需要配置批量SQL注入器）
                for (ChatMessageEntity entity : messageEntities) {
                    chatMessageMapper.insert(entity);
                }
                // 如果有批量插入方法，可以优化为：
                // chatMessageMapper.batchInsert(messageEntities);
            }

            return successCount;

        } catch (Exception e) {
            log.error("批量保存消息到数据库失败", e);

            // 发生异常时，将所有消息标记为失败（需要重试）
            for (RedisMessageEntity redisMessage : redisMessages) {
                redisMessageManager.markSyncFailed(redisMessage);
            }

            return 0;
        }
    }

    /**
     * 转换Redis消息为数据库实体
     */
    private ChatMessageEntity convertToEntity(RedisMessageEntity redisMessage) {
        return ChatMessageEntity.builder()
                .sessionId(redisMessage.getSessionId())
                .messageId(redisMessage.getMessageId() != null ?
                        redisMessage.getMessageId() : UUID.randomUUID().toString())
                .role(redisMessage.getRole())
                .content(redisMessage.getContent())
                .contentType(redisMessage.getContentType() != null ?
                        redisMessage.getContentType() : "text")
                .tokenCount(redisMessage.getTokenCount() != null ?
                        redisMessage.getTokenCount() : 0)
                .modelName(redisMessage.getModelName())
                .createdAt(redisMessage.getCreatedAt() != null ?
                        redisMessage.getCreatedAt() : LocalDateTime.now())
                .build();
    }

    /**
     * 更新会话统计
     */
    private void updateSessionStats(String sessionId, RedisMessageEntity message) {
        try {
            // 检查会话是否存在
            LambdaQueryWrapper<ChatSessionEntity> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.eq(ChatSessionEntity::getSessionId, sessionId);
            ChatSessionEntity session = chatSessionMapper.selectOne(queryWrapper);

            if (session == null) {
                // 创建新会话
                session = ChatSessionEntity.builder()
                        .sessionId(sessionId)
                        .sessionType("agent")
                        .status(1)
                        .messageCount(1)
                        .tokenCount(message.getTokenCount() != null ? message.getTokenCount() : 0)
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .build();
                chatSessionMapper.insert(session);
            } else {
                // 更新现有会话
                chatSessionMapper.incrementMessageCount(sessionId);
                if (message.getTokenCount() != null && message.getTokenCount() > 0) {
                    // 如果需要更新token总数，可以实现相应的方法
                    // chatSessionMapper.addTokenCount(sessionId, message.getTokenCount());
                }
            }
        } catch (Exception e) {
            log.error("更新会话统计失败：sessionId={}", sessionId, e);
        }
    }

    /**
     * 手动触发同步（用于测试或管理）
     */
    public CompletableFuture<Integer> manualSync(int count) {
        return CompletableFuture.supplyAsync(() -> {
            List<RedisMessageEntity> messages = redisMessageManager.getPendingMessages(count);
            if (messages.isEmpty()) {
                return 0;
            }
            return batchSaveToDatabase(messages);
        });
    }

    /**
     * 获取同步统计信息
     */
    public SyncStats getSyncStats() {
        return SyncStats.builder()
                .totalSynced(syncCount.get())
                .totalFailed(failCount.get())
                .pendingCount(redisMessageManager.getPendingMessageCount())
                .activeSessions(redisMessageManager.getActiveSessions().size())
                .build();
    }

    /**
     * 同步统计信息
     */
    @Data
    @Builder
    public static class SyncStats {
        private int totalSynced;
        private int totalFailed;
        private long pendingCount;
        private int activeSessions;
    }
}