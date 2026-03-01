//package com.pufferfishscheduler.ai.agent.service.impl;
//
//import com.github.benmanes.caffeine.cache.Cache;
//import com.github.benmanes.caffeine.cache.Caffeine;
//import com.pufferfishscheduler.ai.agent.service.ChatMessageRedisManager;
//import com.pufferfishscheduler.ai.agent.service.ChatService;
//import com.pufferfishscheduler.ai.agent.service.ChatSessionService;
//import com.pufferfishscheduler.ai.agent.vo.MessageVo;
//import com.pufferfishscheduler.common.constants.Constants;
//import com.pufferfishscheduler.domain.model.RedisMessageEntity;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.ai.chat.client.ChatClient;
//import org.springframework.stereotype.Service;
//import reactor.core.publisher.Flux;
//
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * 聊天服务实现类
// * 只负责业务逻辑，不再直接操作数据库
// */
//@Slf4j
//@Service
//@RequiredArgsConstructor
//public class ChatServiceImpl implements ChatService {
//
//    // 只保留必要的依赖
//    private final ChatSessionService chatSessionService;  // 只用于会话管理
//    private final ChatMessageRedisManager redisMessageManager;  // Redis消息管理
//    private final ChatClient chatClient;
//
//    // 常量定义
//    private static final int CACHE_MAX_MESSAGES = 100;
//    private static final int CACHE_MAX_SESSIONS = 1000;
//    private static final Duration MESSAGE_CACHE_EXPIRE = Duration.ofMinutes(30);
//
//    // 本地消息缓存 - 用于快速读取
//    private final Cache<String, List<MessageVo>> localMessageCache = Caffeine.newBuilder()
//            .maximumSize(CACHE_MAX_SESSIONS)
//            .expireAfterWrite(MESSAGE_CACHE_EXPIRE)
//            .recordStats()
//            .softValues()
//            .build();
//
//    @Override
//    public Flux<String> ask(String conversationId, String prompt) {
//        // 严格的空值校验
//        if (conversationId == null || conversationId.isBlank()) {
//            log.error("会话ID为空，无法发起AI询问");
//            return Flux.just(buildErrorJson("会话ID不能为空"));
//        }
//        if (prompt == null || prompt.isBlank()) {
//            log.error("提示词为空，无法发起AI询问，conversationId: {}", conversationId);
//            return Flux.just(buildErrorJson("提示词不能为空"));
//        }
//
//        try {
//            // 1. 保存用户消息到Redis
//            saveUserMessageToRedis(conversationId, prompt);
//
//            // 2. 创建StringBuilder收集AI回复
//            StringBuilder aiResponseBuilder = new StringBuilder();
//
//            // 3. 流式调用AI并处理响应
//            return chatClient.prompt()
//                    .user(prompt)
//                    .advisors(a -> a.param(Constants.CHAT_MEMORY_CONVERSATION_ID_KEY, conversationId))
//                    .stream()
//                    .content()
//                    .doOnNext(content -> {
//                        if (content != null && !content.isBlank()) {
//                            aiResponseBuilder.append(content);
//                        }
//                    })
//                    .doOnComplete(() -> {
//                        String aiResponse = aiResponseBuilder.toString().trim();
//                        if (!aiResponse.isEmpty()) {
//                            // 保存AI回复到Redis
//                            saveAiMessageToRedis(conversationId, aiResponse);
//                            log.info("AI回复保存到Redis成功，conversationId: {}, 回复长度: {}",
//                                    conversationId, aiResponse.length());
//                        }
//                    })
//                    .doOnError(error -> {
//                        log.error("AI响应过程中发生错误，conversationId: {}", conversationId, error);
//                        saveAiMessageToRedis(conversationId,
//                                String.format("抱歉，处理您的请求时出现错误: %s", error.getMessage()));
//                    });
//        } catch (Exception e) {
//            log.error("发起AI询问失败，conversationId: {}, prompt: {}", conversationId, prompt, e);
//            return Flux.just(buildErrorJson("发起AI询问失败：" + e.getMessage()));
//        }
//    }
//
//    /**
//     * 保存用户消息到Redis
//     */
//    private void saveUserMessageToRedis(String conversationId, String content) {
//        RedisMessageEntity redisMessage = RedisMessageEntity.builder()
//                .sessionId(conversationId)
//                .messageId(UUID.randomUUID().toString())
//                .role(Constants.MESSAGE_TYPE.USER)
//                .content(content == null ? "" : content)
//                .contentType("text")
//                .tokenCount(estimateTokenCount(content))
//                .createdAt(LocalDateTime.now())
//                .syncStatus(0)  // 待同步
//                .retryCount(0)
//                .build();
//
//        redisMessageManager.saveMessage(redisMessage);
//
//        // 更新本地缓存
//        updateLocalMessageCache(conversationId, convertToMessageVo(redisMessage));
//    }
//
//    /**
//     * 保存AI回复到Redis
//     */
//    private void saveAiMessageToRedis(String conversationId, String content) {
//        RedisMessageEntity redisMessage = RedisMessageEntity.builder()
//                .sessionId(conversationId)
//                .messageId(UUID.randomUUID().toString())
//                .role(Constants.MESSAGE_TYPE.ASSISTANT)
//                .content(content == null ? "" : content)
//                .contentType("text")
//                .tokenCount(estimateTokenCount(content))
//                .modelName("qwen-plus")
//                .createdAt(LocalDateTime.now())
//                .syncStatus(0)  // 待同步
//                .retryCount(0)
//                .build();
//
//        redisMessageManager.saveMessage(redisMessage);
//
//        // 更新本地缓存
//        updateLocalMessageCache(conversationId, convertToMessageVo(redisMessage));
//    }
//
//    /**
//     * 转换Redis消息为MessageVo
//     */
//    private MessageVo convertToMessageVo(RedisMessageEntity entity) {
//        if (entity == null) return null;
//        return MessageVo.builder()
//                .role(entity.getRole())
//                .content(entity.getContent())
//                .timestamp(entity.getCreatedAt())
//                .build();
//    }
//
//    /**
//     * 更新本地消息缓存
//     */
//    private void updateLocalMessageCache(String conversationId, MessageVo message) {
//        List<MessageVo> cachedMessages = localMessageCache.get(conversationId, k -> new ArrayList<>());
//        synchronized (cachedMessages) {
//            cachedMessages.add(message);
//            if (cachedMessages.size() > CACHE_MAX_MESSAGES) {
//                int startIndex = cachedMessages.size() - CACHE_MAX_MESSAGES;
//                List<MessageVo> limitedMessages = new ArrayList<>(cachedMessages.subList(startIndex, cachedMessages.size()));
//                localMessageCache.put(conversationId, limitedMessages);
//            } else {
//                localMessageCache.put(conversationId, cachedMessages);
//            }
//        }
//    }
//
//    @Override
//    public void addSessionId(String type, String conversationId) {
//        if (type == null || type.isBlank() || conversationId == null || conversationId.isBlank()) {
//            log.error("参数无效，无法添加会话ID");
//            return;
//        }
//        // 只记录到Redis，不直接操作数据库
//        redisMessageManager.addActiveSession(conversationId);
//    }
//
//    @Override
//    public List<String> getSessionIds(String type) {
//        if (type == null || type.isBlank()) {
//            return Collections.emptyList();
//        }
//        // 从Redis获取活跃会话
//        return new ArrayList<>(redisMessageManager.getActiveSessions());
//    }
//
//    @Override
//    public List<MessageVo> getSessionHistory(String type, String conversationId) {
//        if (conversationId == null || conversationId.isBlank()) {
//            return Collections.emptyList();
//        }
//
//        // 1. 先从本地缓存获取
//        List<MessageVo> cachedMessages = localMessageCache.getIfPresent(conversationId);
//        if (cachedMessages != null && !cachedMessages.isEmpty()) {
//            log.debug("从本地缓存获取消息历史: {}", conversationId);
//            return new ArrayList<>(cachedMessages);
//        }
//
//        // 2. 从Redis获取
//        List<RedisMessageEntity> redisMessages = redisMessageManager.getSessionMessages(conversationId);
//        if (!redisMessages.isEmpty()) {
//            List<MessageVo> messages = redisMessages.stream()
//                    .map(this::convertToMessageVo)
//                    .filter(Objects::nonNull)
//                    .sorted(Comparator.comparing(MessageVo::getTimestamp))
//                    .collect(Collectors.toList());
//
//            // 回填到本地缓存
//            localMessageCache.put(conversationId, messages);
//            return messages;
//        }
//
//        return Collections.emptyList();
//    }
//
//    /**
//     * 估算Token数量
//     */
//    private int estimateTokenCount(String content) {
//        if (content == null || content.isBlank()) {
//            return 0;
//        }
//        int chineseCharCount = content.replaceAll("[^\\u4e00-\\u9fa5]", "").length();
//        int otherCharCount = content.length() - chineseCharCount;
//        return chineseCharCount + (otherCharCount / 4);
//    }
//
//    /**
//     * 构建错误JSON
//     */
//    private String buildErrorJson(String message) {
//        return String.format("{\"error\": true, \"message\": \"%s\"}",
//                message.replace("\"", "\\\""));
//    }
//}