package com.pufferfishscheduler.ai.repository;

import com.pufferfishscheduler.ai.agent.service.ChatMessageRedisManager;
import com.pufferfishscheduler.domain.model.RedisMessageEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Redis + 数据库双重存储的ChatMemoryRepository
 * 消息先存Redis，然后定时同步到数据库
 *
 * @author Mayc
 * @since 2026-03-01
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DatabaseChatMemoryRepository implements ChatMemoryRepository {


    // 只依赖Redis管理器，不再直接依赖Mapper
    private final ChatMessageRedisManager redisMessageManager;

    // 本地缓存，提高读取性能
    private final ConcurrentHashMap<String, List<Message>> localCache = new ConcurrentHashMap<>();

    /**
     * 查询所有会话ID
     * 从Redis获取活跃会话
     */
    @Override
    public List<String> findConversationIds() {
        try {
            Set<String> activeSessions = redisMessageManager.getActiveSessions();
            log.debug("从Redis查询到会话ID数量: {}", activeSessions.size());
            return new ArrayList<>(activeSessions);
        } catch (Exception e) {
            log.error("查询会话ID列表失败", e);
            return new ArrayList<>();
        }
    }

    /**
     * 根据会话ID查询消息
     * 优先从Redis获取，没有则从数据库加载（通过同步服务）
     */
    @Override
    public List<Message> findByConversationId(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            log.error("会话ID为空，无法查询消息");
            return new ArrayList<>();
        }

        try {
            // 1. 先检查本地缓存
            List<Message> cachedMessages = localCache.get(conversationId);
            if (cachedMessages != null && !cachedMessages.isEmpty()) {
                log.debug("从本地缓存获取消息: conversationId={}, 消息数={}",
                        conversationId, cachedMessages.size());
                return new ArrayList<>(cachedMessages);
            }

            // 2. 从Redis获取
            List<RedisMessageEntity> redisMessages = redisMessageManager.getSessionMessages(conversationId);

            if (!redisMessages.isEmpty()) {
                List<Message> messages = redisMessages.stream()
                        .map(this::convertToSpringMessage)
                        .filter(Objects::nonNull)
                        .sorted(Comparator.comparing(m -> {
                            // 从消息内容中提取时间（这里简化处理，实际可以从实体中获取）
                            return 0;
                        }))
                        .collect(Collectors.toList());

                // 更新本地缓存
                if (!messages.isEmpty()) {
                    localCache.put(conversationId, new ArrayList<>(messages));
                }

                log.debug("从Redis获取消息: conversationId={}, 消息数={}",
                        conversationId, messages.size());
                return messages;
            }

            // 3. Redis中没有，返回空列表（不再直接从数据库加载）
            // 因为未同步的消息应该在Redis中，已同步的消息也会在Redis中有缓存
            log.debug("Redis中没有找到消息: conversationId={}", conversationId);
            return new ArrayList<>();

        } catch (Exception e) {
            log.error("获取消息失败: conversationId={}", conversationId, e);
            return new ArrayList<>();
        }
    }

    /**
     * 批量保存消息
     * 保存到Redis，等待定时同步到数据库
     */
    @Override
    public void saveAll(String conversationId, List<Message> messages) {
        if (conversationId == null || conversationId.isBlank()) {
            log.error("会话ID为空，无法保存消息");
            return;
        }
        if (messages == null || messages.isEmpty()) {
            log.debug("保存空消息列表，跳过: conversationId={}", conversationId);
            return;
        }

        try {
            // 1. 转换为Redis实体
            List<RedisMessageEntity> redisMessages = new ArrayList<>();
            LocalDateTime now = LocalDateTime.now();

            for (Message message : messages) {
                if (message == null) {
                    continue;
                }

                RedisMessageEntity redisMessage = convertToRedisEntity(conversationId, message, now);
                if (redisMessage != null) {
                    redisMessages.add(redisMessage);
                }
            }

            // 2. 批量保存到Redis
            if (!redisMessages.isEmpty()) {
                redisMessageManager.batchSaveMessages(redisMessages);

                // 3. 更新本地缓存
                updateLocalCache(conversationId, messages);

                log.debug("消息已保存到Redis: conversationId={}, 消息数={}",
                        conversationId, redisMessages.size());
            }

        } catch (Exception e) {
            log.error("保存消息到Redis失败: conversationId={}", conversationId, e);
        }
    }

    /**
     * 删除会话消息
     */
    @Override
    public void deleteByConversationId(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            log.error("会话ID为空，无法删除消息");
            return;
        }

        try {
            // 1. 清除本地缓存
            localCache.remove(conversationId);

            // 2. 删除Redis中的会话数据
            redisMessageManager.deleteSession(conversationId);

            log.info("已删除会话数据: conversationId={}", conversationId);
        } catch (Exception e) {
            log.error("删除会话数据失败: conversationId={}", conversationId, e);
        }
    }

    /**
     * 转换为Spring AI Message
     */
    private Message convertToSpringMessage(RedisMessageEntity entity) {
        try {
            if (entity == null || entity.getContent() == null) {
                return null;
            }

            String content = entity.getContent();
            return switch (entity.getRole()) {
                case "user" -> new UserMessage(content);
                case "assistant" -> new AssistantMessage(content);
                case "system" -> new SystemMessage(content);
                default -> {
                    log.warn("未知的消息角色: {}", entity.getRole());
                    yield null;
                }
            };
        } catch (Exception e) {
            log.error("转换消息失败: entity={}", entity, e);
            return null;
        }
    }

    /**
     * 转换为Redis实体
     */
    private RedisMessageEntity convertToRedisEntity(String conversationId, Message message, LocalDateTime now) {
        if (message == null) {
            return null;
        }

        String role = getMessageRole(message);
        String content = getMessageContent(message);

        return RedisMessageEntity.builder()
                .sessionId(conversationId)
                .messageId(UUID.randomUUID().toString())
                .role(role)
                .content(content)
                .contentType("text")
                .tokenCount(estimateTokenCount(content))
                .modelName(getModelName(role))  // 只有assistant消息才有modelName
                .createdAt(now)
                .syncStatus(0)  // 待同步
                .retryCount(0)
                .build();
    }

    /**
     * 获取消息角色
     */
    private String getMessageRole(Message message) {
        if (message instanceof UserMessage) {
            return "user";
        } else if (message instanceof AssistantMessage) {
            return "assistant";
        } else if (message instanceof SystemMessage) {
            return "system";
        }
        return "unknown";
    }

    /**
     * 获取消息内容
     */
    private String getMessageContent(Message message) {
        if (message == null) {
            return "";
        }
        String text = message.getText();
        return text == null ? "" : text;
    }

    /**
     * 获取模型名称
     */
    private String getModelName(String role) {
        return "assistant".equals(role) ? "qwen-plus" : null;
    }

    /**
     * 估算Token数量
     */
    private int estimateTokenCount(String content) {
        if (content == null || content.isBlank()) {
            return 0;
        }
        int chineseCharCount = content.replaceAll("[^\\u4e00-\\u9fa5]", "").length();
        int otherCharCount = content.length() - chineseCharCount;
        return chineseCharCount + (otherCharCount / 4);
    }

    /**
     * 更新本地缓存
     */
    private void updateLocalCache(String conversationId, List<Message> messages) {
        localCache.compute(conversationId, (k, v) -> {
            List<Message> list = v == null ? new ArrayList<>() : new ArrayList<>(v);
            list.addAll(messages);
            // 限制缓存大小，只保留最近的100条
            if (list.size() > 100) {
                int startIndex = list.size() - 100;
                return new ArrayList<>(list.subList(startIndex, list.size()));
            }
            return list;
        });
    }

    /**
     * 清理本地缓存
     */
    public void cleanupCache() {
        int beforeSize = localCache.size();
        localCache.clear();
        log.info("清理本地缓存完成，清理前大小: {}, 清理后大小: {}", beforeSize, localCache.size());
    }

    /**
     * 获取缓存统计
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("cacheSize", localCache.size());
        stats.put("cachedConversations", new ArrayList<>(localCache.keySet()));
        return stats;
    }
}