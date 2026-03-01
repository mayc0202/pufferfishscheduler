package com.pufferfishscheduler.ai.agent.service.impl;

import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.ai.agent.service.AgentService;
import com.pufferfishscheduler.common.enums.UserIntent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Agent Service - 修复类型转换问题
 *
 * @author Mayc
 * @since 2026-02-24 13:43
 */
@Slf4j
@Service
public class AgentServiceImpl implements AgentService {

    private final CompiledGraph agentGraph;
    private final ObjectMapper objectMapper;

    private final ChatMemoryRepository chatMemoryRepository;

    private static final long CHAR_DELAY_MS = 20;

    public AgentServiceImpl(@Qualifier("agentGraph") CompiledGraph agentGraph,
                            MessageWindowChatMemory messageWindowChatMemory,
                            ChatMemoryRepository chatMemoryRepository
                            ) {
        this.agentGraph = agentGraph;
        this.objectMapper = new ObjectMapper();

        this.chatMemoryRepository = chatMemoryRepository;
    }

    @Override
    public Flux<String> askAgent(String conversationId, String question) {
        return Flux.create(sink -> {
            try {
                // 1. 存储用户消息
                UserMessage userMessage = new UserMessage(question);
                // TODO: 2026-02-24 14:00 临时方案，后续需要优化
                chatMemoryRepository.saveAll(conversationId, List.of(userMessage));
                log.info("已存储用户消息到会话: {}", conversationId);

                // 2. 发送开始消息
                sendMessage(sink, "start", "开始处理您的请求...");

                // 3. 调用 agentGraph
                Optional<OverAllState> overAllStateOptional = agentGraph.call(Map.of("question", question));

                if (overAllStateOptional.isEmpty()) {
                    sendMessage(sink, "error", "未获取到有效响应");
                    sendDone(sink);
                    sink.complete();
                    return;
                }

                Map<String, Object> result = overAllStateOptional.get().data();

                // 4. 获取响应内容
                String response = null;
                Object responseObj = result.get("response");
                if (responseObj != null) {
                    response = responseObj.toString();
                }

                // 5. 获取意图
                String intentCode = "unknown";
                Object intentObj = result.get("userIntent");
                if (intentObj != null) {
                    if (intentObj instanceof UserIntent) {
                        intentCode = ((UserIntent) intentObj).getCode();
                    } else {
                        intentCode = intentObj.toString();
                    }
                }

                // 6. 获取查询结果
                Integer rowCount = null;
                Object rowCountObj = result.get("rowCount");
                if (rowCountObj instanceof Number) {
                    rowCount = ((Number) rowCountObj).intValue();
                }

                log.info("Agent处理完成 - 意图: {}, 响应长度: {}, 行数: {}",
                        intentCode, response != null ? response.length() : 0, rowCount);

                // 7. 如果没有自然语言回答，生成简单回答
                if ((response == null || response.isEmpty()) && rowCount != null) {
                    response = generateSimpleResponse(question, rowCount, result);
                }

                // 8. 存储助手消息
                if (response != null && !response.isEmpty()) {
                    AssistantMessage assistantMessage = new AssistantMessage(response);
                    // TODO: 2026-02-24 14:00 临时方案，后续需要优化
                    chatMemoryRepository.saveAll(conversationId, List.of(assistantMessage));
                    log.info("已存储助手消息到会话: {}", conversationId);

                    // 9. 流式输出
                    for (char c : response.toCharArray()) {
                        Map<String, Object> tokenMsg = new HashMap<>();
                        tokenMsg.put("type", "token");
                        tokenMsg.put("content", String.valueOf(c));

                        sink.next("data: " + objectMapper.writeValueAsString(tokenMsg) + "\n\n");

                        try {
                            Thread.sleep(CHAR_DELAY_MS);
                        } catch (InterruptedException e) {
                            log.error("Stream interrupted", e);
                            Thread.currentThread().interrupt();
                            sendMessage(sink, "error", "流式输出中断");
                            sendDone(sink);
                            sink.complete();
                            return;
                        }
                    }
                } else {
                    response = "查询完成，但未生成回答内容。";
                    // 也存储默认响应
                    AssistantMessage assistantMessage = new AssistantMessage(response);
                    // TODO: 2026-02-24 14:00 临时方案，后续需要优化
                    chatMemoryRepository.saveAll(conversationId, List.of(assistantMessage));
                    sendMessage(sink, "response", response);
                }

                // 10. 发送完整数据
                Map<String, Object> completeMsg = new HashMap<>();
                completeMsg.put("type", "complete");
                completeMsg.put("response", response);
                completeMsg.put("intent", intentCode);
                completeMsg.put("rowCount", rowCount);
                completeMsg.put("hasChart", result.containsKey("chartConfig"));
                completeMsg.put("conversationId", conversationId);

                // 如果有图表配置，也返回
                if (result.containsKey("chartConfig")) {
                    completeMsg.put("chartConfig", result.get("chartConfig"));
                }

                sink.next("data: " + objectMapper.writeValueAsString(completeMsg) + "\n\n");

                sendDone(sink);
                sink.complete();

            } catch (Exception e) {
                log.error("Error in askAgent stream", e);
                handleError(sink, e);

                // 错误时也可以记录一条错误消息
                try {
                    AssistantMessage errorMessage = new AssistantMessage("处理请求时发生错误: " + e.getMessage());
                    // TODO: 2026-02-24 14:00 临时方案，后续需要优化
                    chatMemoryRepository.saveAll(conversationId, List.of(errorMessage));
                } catch (Exception ex) {
                    log.error("存储错误消息失败", ex);
                }
            }
        });
    }

    /**
     * 生成简单的响应（当大模型超时时使用）
     */
    private String generateSimpleResponse(String question, int rowCount, Map<String, Object> result) {
        StringBuilder sb = new StringBuilder();

        if (question.contains("总数") || question.contains("count") || question.contains("统计")) {
            sb.append("统计完成，共查询到 ").append(rowCount).append(" 条记录。");

            // 如果有分组数据，可以简单展示
            if (result.containsKey("data") && result.get("data") instanceof Iterable) {
                sb.append("\n\n分组结果：\n");
                int i = 1;
                for (Object row : (Iterable<?>) result.get("data")) {
                    if (i > 5) {
                        sb.append("... 等 ").append(rowCount).append(" 条记录");
                        break;
                    }
                    sb.append(i).append(". ").append(row).append("\n");
                    i++;
                }
            }
        } else {
            sb.append("查询完成，共找到 ").append(rowCount).append(" 条记录。");
        }

        return sb.toString();
    }

    private void sendMessage(reactor.core.publisher.FluxSink<String> sink, String type, String content) {
        try {
            Map<String, Object> msg = new HashMap<>();
            msg.put("type", type);
            msg.put("content", content);
            msg.put("timestamp", System.currentTimeMillis());

            sink.next("data: " + objectMapper.writeValueAsString(msg) + "\n\n");
        } catch (Exception e) {
            log.error("发送消息失败", e);
        }
    }

    private void sendDone(reactor.core.publisher.FluxSink<String> sink) {
        sink.next("data: [DONE]\n\n");
    }

    private void handleError(reactor.core.publisher.FluxSink<String> sink, Exception e) {
        try {
            Map<String, Object> errorMsg = new HashMap<>();
            errorMsg.put("type", "error");
            errorMsg.put("content", "处理请求时发生错误: " + e.getMessage());
            errorMsg.put("timestamp", System.currentTimeMillis());

            sink.next("data: " + objectMapper.writeValueAsString(errorMsg) + "\n\n");
            sendDone(sink);
        } catch (Exception ex) {
            log.error("Error sending error message", ex);
        }
        sink.error(e);
    }

    /**
     * 获取历史消息记录
     *
     * @param conversationId
     * @return
     */
    @Override
    public List<Message> getHistory(String conversationId) {
        return chatMemoryRepository.findByConversationId(conversationId);
    }

    /**
     * 清除会话历史
     *
     * @param conversationId 会话ID
     */
    @Override
    public void clearHistory(String conversationId) {
        chatMemoryRepository.deleteByConversationId(conversationId);
        log.info("已清除会话历史: {}", conversationId);
    }
}