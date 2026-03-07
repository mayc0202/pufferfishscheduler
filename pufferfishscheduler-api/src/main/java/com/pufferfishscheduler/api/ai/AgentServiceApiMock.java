package com.pufferfishscheduler.api.ai;

import com.pufferfishscheduler.domain.wrapper.SerializableFluxWrapper;
import org.springframework.ai.chat.messages.Message;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * 自定义Mock返回值
 *
 * @author Mayc
 * @since 2026-03-04  10:48
 */
public class AgentServiceApiMock implements AgentServiceApi {

    @Override
    public SerializableFluxWrapper askAgent(String conversationId, String question) {
        throw new IllegalStateException("服务暂时不可用，请稍后重试");
    }

    @Override
    public List<Message> getHistory(String conversationId) {
        throw new IllegalStateException("服务暂时不可用，请稍后重试");
    }

    @Override
    public void clearHistory(String conversationId) {
        throw new IllegalStateException("服务暂时不可用，请稍后重试");
    }

    @Override
    public void fileChunk(MultipartFile file) {
        throw new IllegalStateException("服务暂时不可用，请稍后重试");
    }
}
