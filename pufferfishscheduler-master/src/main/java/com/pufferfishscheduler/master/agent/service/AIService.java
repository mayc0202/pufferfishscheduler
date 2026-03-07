package com.pufferfishscheduler.master.agent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import org.springframework.ai.chat.messages.Message;
import com.pufferfishscheduler.api.ai.AgentServiceApi;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Mayc
 * @since 2026-03-03  00:35
 */
@Slf4j
@Service
public class AIService {

    @DubboReference(
            version = "1.0.0",
            group = "ai",
            mock = "com.pufferfishscheduler.api.ai.AgentServiceApiMock"
    )
    private AgentServiceApi agentServiceApi;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 提问智能体
     *
     * @param conversationId 会话ID
     * @param question       问题
     * @return 流式响应
     */
    public Flux<String> askAgent(String conversationId, String question) {
        log.info("Master调用AI服务: conversationId={}, question={}", conversationId, question);
        // 调用Dubbo服务获取完整结果
        SerializableFluxWrapper wrapper = agentServiceApi.askAgent(conversationId, question);

        if (wrapper == null || wrapper.getContent() == null) {
            return Flux.just(
                    "data: " + "{\"type\":\"error\",\"content\":\"未获取到响应\"}\n\n",
                    "data: [DONE]\n\n"
            );
        }

        String content = wrapper.getContent();

        // 将每个字符转换为SSE格式
        return Flux.fromStream(
                        content.chars()
                                .mapToObj(c -> {
                                    try {
                                        Map<String, Object> tokenMsg = new HashMap<>();
                                        tokenMsg.put("type", "token");
                                        tokenMsg.put("content", String.valueOf((char) c));
                                        return "data: " + objectMapper.writeValueAsString(tokenMsg) + "\n\n";
                                    } catch (JsonProcessingException e) {
                                        log.error("JSON转换失败", e);
                                        return "data: " + "{\"type\":\"error\",\"content\":\"JSON转换失败\"}\n\n";
                                    }
                                })
                )
                .delayElements(Duration.ofMillis(30))
                .concatWithValues(
                        createCompleteMessage(wrapper, content),
                        "data: [DONE]\n\n"
                );
    }

    /**
     * 创建完成消息
     *
     * @param wrapper
     * @param content
     * @return
     */
    private String createCompleteMessage(SerializableFluxWrapper wrapper, String content) {
        try {
            Map<String, Object> completeMsg = new HashMap<>();
            completeMsg.put("type", "complete");
            completeMsg.put("content", content);
            completeMsg.put("intent", wrapper.getIntentCode());
            completeMsg.put("rowCount", wrapper.getRowCount());
            completeMsg.put("hasChart", wrapper.getHasChart());
            completeMsg.put("chartConfig", wrapper.getChartConfig());
            completeMsg.put("format", wrapper.getFormat());

            return "data: " + objectMapper.writeValueAsString(completeMsg) + "\n\n";
        } catch (JsonProcessingException e) {
            log.error("JSON转换失败", e);
            return "data: " + "{\"type\":\"error\",\"content\":\"JSON转换失败\"}\n\n";
        }
    }

    /**
     * 获取历史消息
     *
     * @param conversationId 会话ID
     * @return 历史消息列表
     */
    public List<Message> getHistory(String conversationId) {
        log.info("Master获取AI历史消息: conversationId={}", conversationId);
        return agentServiceApi.getHistory(conversationId);
    }

    /**
     * 清空历史消息
     *
     * @param conversationId 会话ID
     */
    public void clearHistory(String conversationId) {
        log.info("Master清空AI历史消息: conversationId={}", conversationId);
        agentServiceApi.clearHistory(conversationId);
    }

    /**
     * 文件切片
     *
     * @param file
     */
    public void fileChunk(MultipartFile file) {
        log.info("Master文件切片: file={}", file.getOriginalFilename());
        agentServiceApi.fileChunk(file);
    }
}
