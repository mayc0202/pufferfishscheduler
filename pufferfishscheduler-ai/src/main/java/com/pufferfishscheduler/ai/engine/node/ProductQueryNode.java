package com.pufferfishscheduler.ai.engine.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 产品查询节点（检索知识库中产品信息）
 *
 * @author Mayc
 * @since 2026-02-24  11:34
 */
@Slf4j
public class ProductQueryNode implements NodeAction {

    private static final String PROMPT_TEMPLATE = """
            基于以下产品信息回答用户问题。
            
                产品信息：
                {product_information}
            
                用户问题：{question}
            
                请用专业、友好的语气回答：""";

    private static final String NO_INFO_TEMPLATE = """
            抱歉，我没有找到关于"{question}"的相关产品信息。
            您可以尝试换个说法提问，或者联系人工客服获取帮助。
            """;

    private final ChatClient chatClient;
    private final VectorStore vectorStore;

    public ProductQueryNode(ChatClient.Builder builder, VectorStore vectorStore) {
        this.chatClient = builder.build();
        this.vectorStore = vectorStore;
    }

    @Override
    public Map<String, Object> apply(OverAllState state) throws Exception {
        String question = Optional.ofNullable(state.value("question"))
                .map(Object::toString)
                .orElse("");

        if (question.trim().isEmpty()) {
            log.warn("用户输入为空!");
            return Map.of("response", "请提供有效的问题！");
        }

        log.info("开始知识库检索，用户输入：{}", question);

        String result;
        try {
            // 执行相似度搜索
            List<Document> docs = vectorStore.similaritySearch(question);

            // 2. 构建上下文信息
            if (docs != null && !docs.isEmpty()) {
                String context = docs.stream()
                        .map(Document::getText)
                        .collect(Collectors.joining("\n"));

                log.info("找到 {} 条相关文档。", docs.size());

                // 创建提示模板
                PromptTemplate promptTemplate = new PromptTemplate(PROMPT_TEMPLATE);
                String prompt = promptTemplate.render(Map.of(
                        "question", question,
                        "product_information", context
                ));

                result = chatClient.prompt()
                        .user(prompt)
                        .call()
                        .content();

            } else {
                log.warn("未找到相关产品信息。");
                PromptTemplate noInfoTemplate = new PromptTemplate(NO_INFO_TEMPLATE);
                result = noInfoTemplate.render(Map.of("question", question));
            }

        } catch (Exception e) {
            log.error("产品查询失败", e);
            result = "抱歉，查询产品信息时遇到技术问题，请稍后再试。";
        }

        return Map.of(
                "response", result,
                "queryType", "product_query"
        );
    }
}
