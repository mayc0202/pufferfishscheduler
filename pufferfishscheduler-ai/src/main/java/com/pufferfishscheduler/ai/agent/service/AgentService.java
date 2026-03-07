package com.pufferfishscheduler.ai.agent.service;

import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import org.springframework.ai.chat.messages.Message;

import java.util.List;

/**
 * 智能体服务接口
 * 提供智能体的基本操作，如处理用户请求、管理会话历史等。
 *
 * @author Mayc
 * @since 2026-03-04  16:32
 */
public interface AgentService {
    /**
     * 向智能体提问
     *
     * @param conversationId 会话ID，用于关联多个问题和回答
     * @param question 用户的问题
     * @return 包含智能体回答的可序列化Flux包装器
     */
     SerializableFluxWrapper askAgent(String conversationId, String question);

    /**
     * 获取会话历史记录
     *
     * @param conversationId 会话ID
     * @return 包含会话历史记录的消息列表
     */
    List<Message> getHistory(String conversationId);

    /**
     * 清除会话历史记录
     *
     * @param conversationId 会话ID
     */
    void clearHistory(String conversationId);
}
