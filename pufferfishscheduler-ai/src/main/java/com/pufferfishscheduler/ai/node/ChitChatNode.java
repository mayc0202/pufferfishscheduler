package com.pufferfishscheduler.ai.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.pufferfishscheduler.common.enums.UserIntent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;

import java.util.Map;
import java.util.Optional;

/**
 * 闲聊节点
 *
 * @author Mayc
 * @since 2026-02-24  14:15
 */
@Slf4j
public class ChitChatNode implements NodeAction {

    private static final String PROMPT_TEMPLATE = """
           你是一个友好的客服助手，请用热情、友好的语气回应用户的闲聊。
           
           用户提问：{question}
           
           你的回复：
           """;

    private final ChatClient chatClient;

    public ChitChatNode(ChatClient.Builder builder) {
        this.chatClient = builder.build();
    }

    @Override
    public Map<String, Object> apply(OverAllState state) throws Exception {
        String question = Optional.ofNullable(state.value("question"))
                .map(Object::toString)
                .orElse("");

        if (question.trim().isEmpty()) {
            log.warn("用户输入为空!");
            // 统一返回userIntent，使用CHITCHAT作为默认意图
            return Map.of(
                    "userIntent", UserIntent.CHITCHAT,
                    "error", "请提供有效的问题"
            );
        }

        log.info("处理闲聊：{}", question);

        PromptTemplate promptTemplate = new PromptTemplate(PROMPT_TEMPLATE);
        promptTemplate.add("question", question);

        String prompt = promptTemplate.render();
        String result = chatClient.prompt()
                .user(prompt)
                .call()
                .content();

        return Map.of(
                "response", result,
                "queryType", "chitchat"
        );
    }
}
