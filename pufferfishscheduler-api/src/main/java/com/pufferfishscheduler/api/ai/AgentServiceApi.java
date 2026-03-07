package com.pufferfishscheduler.api.ai;

import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import org.springframework.ai.chat.messages.Message;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * Agent服务
 *
 * @author Mayc
 * @since 2026-03-03  00:27
 */
public interface AgentServiceApi {


    /**
     * 提问智能体
     *
     * @param conversationId 会话ID
     * @param question       问题
     * @return 流式响应
     */
    SerializableFluxWrapper askAgent(String conversationId, String question);

    /**
     * 获取历史消息
     *
     * @param conversationId 会话ID
     * @return 历史消息列表
     */
    List<Message> getHistory(String conversationId);

    /**
     * 清空历史消息
     *
     * @param conversationId 会话ID
     */
    void clearHistory(String conversationId);

    /**
     * 文件切片
     * @param file
     */
    void fileChunk(MultipartFile file);
}
