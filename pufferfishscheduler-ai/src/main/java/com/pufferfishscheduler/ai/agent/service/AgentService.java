package com.pufferfishscheduler.ai.agent.service;

import org.springframework.ai.chat.messages.Message;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 *
 * @author Mayc
 * @since 2026-02-24  13:43
 */
public interface AgentService {

    /**
     * 提问智能体
     *
     * @param conversationId
     * @param question
     * @return
     */
    Flux<String> askAgent(String conversationId, String question);

    /**
     * 获取历史消息
     *
     * @param conversationId
     * @return
     */
    List<Message> getHistory(String conversationId);

    /**
     * 清空历史消息
     *
     * @param conversationId
     */
    void clearHistory(String conversationId);
}
