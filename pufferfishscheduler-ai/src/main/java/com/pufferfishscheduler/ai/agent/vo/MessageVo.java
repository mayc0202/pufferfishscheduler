package com.pufferfishscheduler.ai.agent.vo;

import com.pufferfishscheduler.common.constants.Constants;
import lombok.*;
import org.springframework.ai.chat.messages.Message;

/**
 *
 * @author Mayc
 * @since 2025-12-16  17:17
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MessageVo {

    /**
     * 角色
     */
    private String role;

    /**
     * 会话内容
     */
    private String content;

    /**
     * 消息ID（可选，用于消息链）
     */
    @Builder.Default
    private String messageId = "";

    /**
     * 父消息ID（可选）
     */
    @Builder.Default
    private String parentMessageId = "";

    /**
     * 内容类型
     */
    @Builder.Default
    private String contentType = "text";

    /**
     * token数量
     */
    @Builder.Default
    private Integer tokenCount = 0;

    /**
     * 时间戳
     */
    @Builder.Default
    private java.time.LocalDateTime timestamp = java.time.LocalDateTime.now();

    /**
     * 元数据（JSON格式）
     */
    @Builder.Default
    private String metadata = "{}";

    /**
     * 从Spring AI Message对象构造
     */
    public MessageVo(Message message) {
        switch (message.getMessageType()) {
            case USER:
                role = Constants.MESSAGE_TYPE.USER;
                break;
            case ASSISTANT:
                role = Constants.MESSAGE_TYPE.ASSISTANT;
                break;
            case SYSTEM:
                role = Constants.MESSAGE_TYPE.SYSTEM;
                break;
            case TOOL:
                role = Constants.MESSAGE_TYPE.TOOL;
                break;
            default:
                role = Constants.MESSAGE_TYPE.UNKNOWN;
                break;
        }

        this.content = message.getText();
        this.timestamp = java.time.LocalDateTime.now();
    }

    /**
     * 使用Builder模式的便捷构造方法（静态工厂方法）
     */
    public static MessageVoBuilder userMessage(String content) {
        return MessageVo.builder()
                .role(Constants.MESSAGE_TYPE.USER)
                .content(content);
    }

    public static MessageVoBuilder assistantMessage(String content) {
        return MessageVo.builder()
                .role(Constants.MESSAGE_TYPE.ASSISTANT)
                .content(content);
    }

    public static MessageVoBuilder systemMessage(String content) {
        return MessageVo.builder()
                .role(Constants.MESSAGE_TYPE.SYSTEM)
                .content(content);
    }

    public static MessageVoBuilder toolMessage(String content) {
        return MessageVo.builder()
                .role(Constants.MESSAGE_TYPE.TOOL)
                .content(content);
    }
}
