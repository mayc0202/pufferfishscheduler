package com.pufferfishscheduler.ai.agent.service.impl;

import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.pufferfishscheduler.ai.agent.service.AgentService;
import com.pufferfishscheduler.common.enums.UserIntent;
import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

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
    private final ChatMemoryRepository chatMemoryRepository;

    private static final long CHAR_DELAY_MS = 20;

    public AgentServiceImpl(@Qualifier("agentGraph") CompiledGraph agentGraph,
                            ChatMemoryRepository chatMemoryRepository
                            ) {
        this.agentGraph = agentGraph;
        this.chatMemoryRepository = chatMemoryRepository;
    }

    @Override
    public SerializableFluxWrapper askAgent(String conversationId, String question) {
        log.info("========== 收到Dubbo调用请求 ==========");
        log.info("调用参数 - conversationId: {}, question: {}", conversationId, question);
        log.info("调用线程: {}", Thread.currentThread().getName());
        log.info("调用时间: {}", System.currentTimeMillis());

        // 用于收集所有流式输出的内容
        StringBuilder fullResponse = new StringBuilder();
        String finalIntentCode = "unknown";
        Integer finalRowCount = null;
        boolean hasChart = false;
        Map<String, Object> chartConfig = null;

        try {
            // 1. 存储用户消息
            UserMessage userMessage = new UserMessage(question);
            chatMemoryRepository.saveAll(conversationId, List.of(userMessage));
            log.info("已存储用户消息到会话: {}", conversationId);

            // 2. 调用 agentGraph
            Optional<OverAllState> overAllStateOptional = agentGraph.call(Map.of("question", question));

            if (overAllStateOptional.isEmpty()) {
                SerializableFluxWrapper wrapper = new SerializableFluxWrapper();
                wrapper.setConversationId(conversationId);
                wrapper.setQuestion(question);
                wrapper.setContent("未获取到有效响应");
                wrapper.setFormat("text");
                wrapper.setSuccess(false);
                wrapper.setErrorMessage("未获取到有效响应");
                wrapper.setTimestamp(System.currentTimeMillis());
                return wrapper;
            }

            Map<String, Object> result = overAllStateOptional.get().data();

            // 3. 获取响应内容
            String response = null;
            Object responseObj = result.get("response");
            if (responseObj != null) {
                response = responseObj.toString();
            }

            // 4. 获取意图
            String intentCode = "unknown";
            Object intentObj = result.get("userIntent");
            if (intentObj != null) {
                if (intentObj instanceof UserIntent) {
                    intentCode = ((UserIntent) intentObj).getCode();
                } else {
                    intentCode = intentObj.toString();
                }
            }
            finalIntentCode = intentCode;

            // 5. 获取查询结果
            Integer rowCount = null;
            Object rowCountObj = result.get("rowCount");
            if (rowCountObj instanceof Number) {
                rowCount = ((Number) rowCountObj).intValue();
            }
            finalRowCount = rowCount;

            // 6. 如果没有自然语言回答，生成简单回答
            if ((response == null || response.isEmpty()) && rowCount != null) {
                response = generateSimpleResponse(question, rowCount, result);
            }

            // 7. 存储助手消息
            if (response != null && !response.isEmpty()) {
                AssistantMessage assistantMessage = new AssistantMessage(response);
                chatMemoryRepository.saveAll(conversationId, List.of(assistantMessage));
                log.info("已存储助手消息到会话: {}", conversationId);

                // 收集完整响应（这里直接使用response，因为已经没有流式输出了）
                fullResponse.append(response);
            } else {
                response = "查询完成，但未生成回答内容。";
                AssistantMessage assistantMessage = new AssistantMessage(response);
                chatMemoryRepository.saveAll(conversationId, List.of(assistantMessage));
                fullResponse.append(response);
            }

            // 8. 检查是否有图表
            hasChart = result.containsKey("chartConfig");
            if (hasChart) {
                chartConfig = (Map<String, Object>) result.get("chartConfig");
            }

            log.info("Agent处理完成 - 意图: {}, 响应长度: {}, 行数: {}",
                    intentCode, fullResponse.length(), rowCount);

        } catch (Exception e) {
            log.error("Error in askAgent", e);

            // 错误时记录错误消息
            try {
                AssistantMessage errorMessage = new AssistantMessage("处理请求时发生错误: " + e.getMessage());
                chatMemoryRepository.saveAll(conversationId, List.of(errorMessage));
            } catch (Exception ex) {
                log.error("存储错误消息失败", ex);
            }

            // 返回错误包装
            SerializableFluxWrapper wrapper = new SerializableFluxWrapper();
            wrapper.setConversationId(conversationId);
            wrapper.setQuestion(question);
            wrapper.setContent("处理请求时发生错误: " + e.getMessage());
            wrapper.setFormat("text");
            wrapper.setSuccess(false);
            wrapper.setErrorMessage(e.getMessage());
            wrapper.setTimestamp(System.currentTimeMillis());
            return wrapper;
        }

        // 构建成功返回的包装对象
        SerializableFluxWrapper wrapper = new SerializableFluxWrapper();
        wrapper.setConversationId(conversationId);
        wrapper.setQuestion(question);
        wrapper.setContent(fullResponse.toString());
        wrapper.setFormat("text");  // 或根据内容判断是否为markdown
        wrapper.setIntentCode(finalIntentCode);
        wrapper.setRowCount(finalRowCount);
        wrapper.setHasChart(hasChart);
        wrapper.setChartConfig(chartConfig);
        wrapper.setSuccess(true);
        wrapper.setTimestamp(System.currentTimeMillis());

        log.info("返回结果: {}", wrapper);
        log.info("========== 调用结束 ==========");

        return wrapper;
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