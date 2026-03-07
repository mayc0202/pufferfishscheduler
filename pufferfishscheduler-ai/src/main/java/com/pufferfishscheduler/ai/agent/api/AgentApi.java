package com.pufferfishscheduler.ai.agent.api;

import com.pufferfishscheduler.ai.agent.service.AgentService;
import com.pufferfishscheduler.ai.agent.service.KnowledgeBaseService;
import com.pufferfishscheduler.api.ai.AgentServiceApi;
import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.ai.chat.messages.Message;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 *
 * @author Mayc
 * @since 2026-03-04  16:28
 */
@Slf4j
@DubboService(
        version = "1.0.0",  // 版本号，用于服务版本管理
        group = "ai",       // 分组，用于区分不同的服务实现
        timeout = 60000,    // 60秒超时，适应AI处理时间
        retries = 3         // 重试3次
)
@AllArgsConstructor
public class AgentApi implements AgentServiceApi {

    private final AgentService agentService;
    private final KnowledgeBaseService knowledgeBaseService;

    /**
     * 向智能体提问
     *
     * @param conversationId 会话ID
     * @param question       问题
     * @return
     */
    @Override
    public SerializableFluxWrapper askAgent(String conversationId, String question) {
        return agentService.askAgent(conversationId, question);
    }

    /**
     * 获取会话历史记录
     * @param conversationId 会话ID
     * @return
     */
    @Override
    public List<Message> getHistory(String conversationId) {
        return agentService.getHistory(conversationId);
    }

    /**
     * 清除会话历史记录
     *
     * @param conversationId 会话ID
     */
    @Override
    public void clearHistory(String conversationId) {
        agentService.clearHistory(conversationId);
    }

    /**
     * 文件切片
     *
     * @param file
     */
    @Override
    public void fileChunk(MultipartFile file) {
        knowledgeBaseService.fileChunk(file);
    }
}
