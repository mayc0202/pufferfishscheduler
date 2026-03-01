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
 * 意图识别节点
 *
 * @author Mayc
 * @since 2026-02-24  11:16
 */
@Slf4j
public class IntentRecognitionNode implements NodeAction {

    private static final String PROMPT_TEMPLATE = """
            请分析以下用户问题的意图，只返回对应的意图代码，不要返回其他内容，不要返回数字。
            
            可能意图：
            1. product_query - 产品相关问题（询问产品规格、价格、功能、使用方法等）
            2. data_asset_query - 数据资产相关问题（查询资源信息等）
            3. chitchat - 闲聊（问候、感谢、非业务相关对话）
            
            用户问题：{question}
            
            请只返回上述三个代码中的一个：product_query、data_asset_operation 或 chitchat
            
            意图代码：""";

    private final ChatClient chatClient;

    public IntentRecognitionNode(ChatClient.Builder builder) {
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

        log.info("开始意图识别，用户输入：{}", question);

        try {
            PromptTemplate promptTemplate = new PromptTemplate(PROMPT_TEMPLATE);
            promptTemplate.add("question", question);

            String prompt = promptTemplate.render();
            String result = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            log.info("意图识别结果：{}", result);

            UserIntent userIntent = result != null ?
                    UserIntent.fromCode(result.trim()) :
                    UserIntent.CHITCHAT;

            return Map.of("userIntent", userIntent);

        } catch (Exception e) {
            log.error("意图识别失败", e);
            // 异常时返回默认意图
            return Map.of(
                    "userIntent", UserIntent.CHITCHAT,
                    "error", "意图识别失败，使用默认意图"
            );
        }
    }
}
