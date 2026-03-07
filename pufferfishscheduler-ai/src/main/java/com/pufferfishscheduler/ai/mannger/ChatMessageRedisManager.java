package com.pufferfishscheduler.ai.mannger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.domain.model.agent.RedisMessageEntity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 会话消息Redis操作工具类
 * 负责消息的Redis存储、读取、删除等操作
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageRedisManager {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    // Redis Key前缀定义
    private static final String REDIS_KEY_PREFIX = "chat:message:";          // 消息存储前缀 (Hash结构)
    private static final String REDIS_KEY_PENDING = "chat:pending:messages"; // 待同步消息ZSet (score=时间戳)
    private static final String REDIS_KEY_SESSIONS = "chat:active:sessions"; // 活跃会话Set
    private static final String REDIS_KEY_SYNC_FAILED = "chat:sync:failed";  // 同步失败消息List

    // 过期时间（7天，可配置）
    private static final Duration MESSAGE_EXPIRE = Duration.ofDays(7);

    // 最大重试次数
    private static final int MAX_RETRY_COUNT = 3;

    /**
     * 保存消息到Redis
     * @param message 消息实体
     */
    public void saveMessage(RedisMessageEntity message) {
        if (message == null || message.getSessionId() == null || message.getMessageId() == null) {
            log.warn("无效的消息参数，跳过Redis存储");
            return;
        }

        try {
            String hashKey = REDIS_KEY_PREFIX + message.getSessionId();
            String messageJson = objectMapper.writeValueAsString(message);

            // 1. 存储消息到Hash
            redisTemplate.opsForHash().put(hashKey, message.getMessageId(), messageJson);

            // 2. 设置过期时间
            redisTemplate.expire(hashKey, MESSAGE_EXPIRE);

            // 3. 记录到待同步队列（score=时间戳）
            String pendingKey = message.getSessionId() + ":" + message.getMessageId();
            redisTemplate.opsForZSet().add(REDIS_KEY_PENDING, pendingKey, System.currentTimeMillis());

            // 4. 记录活跃会话
            redisTemplate.opsForSet().add(REDIS_KEY_SESSIONS, message.getSessionId());

            log.debug("消息已存入Redis：sessionId={}, messageId={}", message.getSessionId(), message.getMessageId());
        } catch (JsonProcessingException e) {
            log.error("消息JSON序列化失败：{}", message, e);
        } catch (Exception e) {
            log.error("存储消息到Redis失败：sessionId={}, messageId={}", message.getSessionId(), message.getMessageId(), e);
        }
    }

    /**
     * 批量保存消息到Redis
     * @param messages 消息列表
     */
    public void batchSaveMessages(List<RedisMessageEntity> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        try {
            // 按会话ID分组
            Map<String, List<RedisMessageEntity>> groupedBySession = messages.stream()
                    .collect(Collectors.groupingBy(RedisMessageEntity::getSessionId));

            for (Map.Entry<String, List<RedisMessageEntity>> entry : groupedBySession.entrySet()) {
                String sessionId = entry.getKey();
                List<RedisMessageEntity> sessionMessages = entry.getValue();

                String hashKey = REDIS_KEY_PREFIX + sessionId;
                Map<String, String> messageMap = new HashMap<>();

                long currentTime = System.currentTimeMillis();

                for (RedisMessageEntity message : sessionMessages) {
                    String messageJson = objectMapper.writeValueAsString(message);
                    messageMap.put(message.getMessageId(), messageJson);

                    // 添加到待同步队列
                    String pendingKey = sessionId + ":" + message.getMessageId();
                    redisTemplate.opsForZSet().add(REDIS_KEY_PENDING, pendingKey, currentTime);
                }

                // 批量存储到Hash
                redisTemplate.opsForHash().putAll(hashKey, messageMap);
                redisTemplate.expire(hashKey, MESSAGE_EXPIRE);

                // 记录活跃会话
                redisTemplate.opsForSet().add(REDIS_KEY_SESSIONS, sessionId);
            }

            log.debug("批量消息已存入Redis：消息数={}", messages.size());
        } catch (Exception e) {
            log.error("批量存储消息到Redis失败", e);
        }
    }

    /**
     * 获取待同步的消息
     * @param limit 获取数量
     * @return 消息实体列表
     */
    public List<RedisMessageEntity> getPendingMessages(int limit) {
        try {
            // 获取最早的N条待同步消息
            Set<Object> pendingKeys = redisTemplate.opsForZSet().range(REDIS_KEY_PENDING, 0, limit - 1);
            if (pendingKeys == null || pendingKeys.isEmpty()) {
                return Collections.emptyList();
            }

            List<RedisMessageEntity> messages = new ArrayList<>();
            Set<String> keysToRemove = new HashSet<>();

            for (Object keyObj : pendingKeys) {
                String key = keyObj.toString();
                String[] parts = key.split(":", 2);
                if (parts.length != 2) {
                    keysToRemove.add(key);
                    continue;
                }

                String sessionId = parts[0];
                String messageId = parts[1];

                String hashKey = REDIS_KEY_PREFIX + sessionId;
                Object messageObj = redisTemplate.opsForHash().get(hashKey, messageId);

                if (messageObj != null) {
                    try {
                        RedisMessageEntity message = objectMapper.readValue(
                                messageObj.toString(),
                                RedisMessageEntity.class
                        );
                        messages.add(message);
                        keysToRemove.add(key);
                    } catch (Exception e) {
                        log.error("解析消息失败：key={}", key, e);
                        // 解析失败的消息也要移除，避免阻塞队列
                        keysToRemove.add(key);
                    }
                } else {
                    // 消息不存在，移除待同步key
                    keysToRemove.add(key);
                }
            }

            // 移除已获取的待同步key
            if (!keysToRemove.isEmpty()) {
                redisTemplate.opsForZSet().remove(REDIS_KEY_PENDING, keysToRemove.toArray());
            }

            return messages;
        } catch (Exception e) {
            log.error("获取待同步消息失败", e);
            return Collections.emptyList();
        }
    }

    /**
     * 标记消息同步失败
     */
    public void markSyncFailed(RedisMessageEntity message) {
        if (message == null) {
            return;
        }

        try {
            message.setRetryCount(message.getRetryCount() + 1);

            if (message.getRetryCount() >= MAX_RETRY_COUNT) {
                // 超过最大重试次数，记录到失败队列
                String messageJson = objectMapper.writeValueAsString(message);
                redisTemplate.opsForList().leftPush(REDIS_KEY_SYNC_FAILED, messageJson);
                log.warn("消息同步失败超过最大重试次数，已转移到失败队列：sessionId={}, messageId={}",
                        message.getSessionId(), message.getMessageId());
            } else {
                // 重新加入待同步队列，使用新的时间戳
                String pendingKey = message.getSessionId() + ":" + message.getMessageId();
                redisTemplate.opsForZSet().add(REDIS_KEY_PENDING, pendingKey, System.currentTimeMillis() + 60000); // 延迟1分钟重试
                log.debug("消息同步失败，准备重试：sessionId={}, messageId={}, retryCount={}",
                        message.getSessionId(), message.getMessageId(), message.getRetryCount());
            }
        } catch (Exception e) {
            log.error("标记消息同步失败出错", e);
        }
    }

    /**
     * 获取同步失败的消息
     */
    public List<RedisMessageEntity> getSyncFailedMessages(int limit) {
        try {
            List<Object> messageObjs = redisTemplate.opsForList().range(REDIS_KEY_SYNC_FAILED, 0, limit - 1);
            if (messageObjs == null || messageObjs.isEmpty()) {
                return Collections.emptyList();
            }

            List<RedisMessageEntity> messages = new ArrayList<>();
            for (Object obj : messageObjs) {
                try {
                    RedisMessageEntity message = objectMapper.readValue(
                            obj.toString(),
                            RedisMessageEntity.class
                    );
                    messages.add(message);
                } catch (Exception e) {
                    log.error("解析失败消息出错", e);
                }
            }

            return messages;
        } catch (Exception e) {
            log.error("获取同步失败消息出错", e);
            return Collections.emptyList();
        }
    }

    /**
     * 移除同步失败的消息
     */
    public void removeSyncFailedMessages(int count) {
        try {
            for (int i = 0; i < count; i++) {
                redisTemplate.opsForList().rightPop(REDIS_KEY_SYNC_FAILED);
            }
        } catch (Exception e) {
            log.error("移除同步失败消息出错", e);
        }
    }

    /**
     * 获取会话的所有消息
     */
    public List<RedisMessageEntity> getSessionMessages(String sessionId) {
        if (sessionId == null || sessionId.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String hashKey = REDIS_KEY_PREFIX + sessionId;
        try {
            Map<Object, Object> rawMap = redisTemplate.opsForHash().entries(hashKey);
            if (rawMap == null || rawMap.isEmpty()) {
                return Collections.emptyList();
            }

            List<RedisMessageEntity> messages = new ArrayList<>();
            for (Object value : rawMap.values()) {
                if (value != null) {
                    try {
                        RedisMessageEntity message = objectMapper.readValue(
                                value.toString(),
                                RedisMessageEntity.class
                        );
                        messages.add(message);
                    } catch (Exception e) {
                        log.error("解析消息失败", e);
                    }
                }
            }

            return messages.stream()
                    .sorted(Comparator.comparing(RedisMessageEntity::getCreatedAt))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("从Redis获取会话消息失败：sessionId={}", sessionId, e);
            return Collections.emptyList();
        }
    }

    /**
     * 获取待同步消息数量
     */
    public long getPendingMessageCount() {
        try {
            Long count = redisTemplate.opsForZSet().size(REDIS_KEY_PENDING);
            return count != null ? count : 0L;
        } catch (Exception e) {
            log.error("获取待同步消息数量失败", e);
            return 0L;
        }
    }

    /**
     * 获取所有活跃会话ID
     */
    public Set<String> getActiveSessions() {
        try {
            Set<Object> rawSet = redisTemplate.opsForSet().members(REDIS_KEY_SESSIONS);
            if (rawSet == null || rawSet.isEmpty()) {
                return Collections.emptySet();
            }

            Set<String> result = new HashSet<>();
            for (Object obj : rawSet) {
                if (obj != null) {
                    result.add(obj.toString());
                }
            }
            return result;
        } catch (Exception e) {
            log.error("获取活跃会话失败", e);
            return Collections.emptySet();
        }
    }

    /**
     * 删除会话的Redis数据
     */
    public void deleteSession(String sessionId) {
        if (sessionId == null || sessionId.trim().isEmpty()) {
            return;
        }

        try {
            // 删除消息Hash
            String hashKey = REDIS_KEY_PREFIX + sessionId;
            redisTemplate.delete(hashKey);

            // 从活跃会话中移除
            redisTemplate.opsForSet().remove(REDIS_KEY_SESSIONS, sessionId);

            log.info("已删除Redis中的会话数据：sessionId={}", sessionId);
        } catch (Exception e) {
            log.error("删除Redis会话数据失败：sessionId={}", sessionId, e);
        }
    }
}