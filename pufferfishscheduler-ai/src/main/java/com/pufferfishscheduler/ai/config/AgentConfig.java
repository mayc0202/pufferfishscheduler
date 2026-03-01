package com.pufferfishscheduler.ai.config;

import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.GraphRepresentation;
import com.alibaba.cloud.ai.graph.KeyStrategyFactory;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.AsyncEdgeAction;
import com.alibaba.cloud.ai.graph.action.AsyncNodeAction;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pufferfishscheduler.ai.node.*;
import com.pufferfishscheduler.ai.repository.DatabaseChatMemoryRepository;
import com.pufferfishscheduler.ai.agent.service.ChatMessageRedisManager;
import com.pufferfishscheduler.common.enums.UserIntent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.rag.advisor.RetrievalAugmentationAdvisor;
import org.springframework.ai.rag.retrieval.search.VectorStoreDocumentRetriever;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.redis.RedisVectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
public class AgentConfig {

    private final Integer defaultTopK = 8; // 检索数量
    private final Double defaultThreshold = 0.3; // 阈值
    private final int MAX_MESSAGES = 100;

    private final Integer timeoutMillis = 30000;
    private final Integer connectionTimeoutMillis = 2000;
    private final Integer socketTimeoutMillis = 2000;

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Value("${spring.data.redis.password}")
    private String password;

    @Value("${spring.data.redis.database}")
    private int database;

    @Value("${spring.ai.vectorstore.redis.initialize-schema}")
    private Boolean initializeSchema;

    @Value("${spring.ai.vectorstore.redis.index-name}")
    private String indexName;

    @Value("${spring.ai.vectorstore.redis.prefix}")
    private String prefix;

    /**
     * 配置RedisTemplate - 用于消息存储
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // 使用StringRedisSerializer来序列化和反序列化redis的key值
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // 使用GenericJackson2JsonRedisSerializer来序列化和反序列化redis的value值
        GenericJackson2JsonRedisSerializer jsonSerializer = new GenericJackson2JsonRedisSerializer();
        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        template.afterPropertiesSet();
        return template;
    }

    // JedisPooled配置 - 用于RedisVectorStore（使用独立的Redis实例）
    @Bean
    public JedisPooled vectorStoreJedisPooled() {

        // 构建JedisClientConfig
        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
                .user("default")  // 设置用户名
                .password(password)  // 设置密码
                .database(database)  // 设置数据库索引
                .timeoutMillis(timeoutMillis) // 命令超时时间
                .connectionTimeoutMillis(connectionTimeoutMillis)  // 连接超时
                .socketTimeoutMillis(socketTimeoutMillis)  // 读写超时
                .build();

        return new JedisPooled(new HostAndPort(host, port), clientConfig);
    }

    // VectorStore配置 - 使用独立的Redis实例
    @Bean
    public VectorStore vectorStore(JedisPooled vectorStoreJedisPooled, EmbeddingModel embeddingModel) {
        return RedisVectorStore.builder(vectorStoreJedisPooled, embeddingModel)
                .indexName(indexName)  // 设置索引名称
                .prefix(prefix)  // 设置key前缀
                .initializeSchema(initializeSchema)  // 自动初始化索引schema
                .vectorAlgorithm(RedisVectorStore.Algorithm.HSNW)  // 设置向量算法
                .build();
    }

    /**
     * 自定义存储 - 注入ChatMessageRedisManager
     */
    @Bean
    @Primary
    public ChatMemoryRepository chatMemoryRepository(ChatMessageRedisManager redisMessageManager) {
        return new DatabaseChatMemoryRepository(redisMessageManager);
    }

    // MessageWindowChatMemory Bean
    @Bean
    public MessageWindowChatMemory messageWindowChatMemory(ChatMemoryRepository chatMemoryRepository) {
        return MessageWindowChatMemory.builder()
                .chatMemoryRepository(chatMemoryRepository)
                .maxMessages(MAX_MESSAGES)
                .build();
    }

    // ChatClient Bean
    @Bean
    public ChatClient chatClient(
            ChatClient.Builder builder,
            VectorStore vectorStore,
            ToolCallbackProvider toolCallbackProvider,
            MessageWindowChatMemory messageWindowChatMemory) {

        log.info("初始化ChatClient，加载的工具类: {}", toolCallbackProvider.getClass().getSimpleName());

        // 创建RAG检索器
        VectorStoreDocumentRetriever retriever = VectorStoreDocumentRetriever.builder()
                .vectorStore(vectorStore)
                .topK(defaultTopK)
                .similarityThreshold(defaultThreshold)
                .build();

        // 创建RAG advisor
        RetrievalAugmentationAdvisor ragAdvisor = RetrievalAugmentationAdvisor.builder()
                .documentRetriever(retriever)
                .build();

        // 创建消息记忆 advisor
        MessageChatMemoryAdvisor memoryAdvisor = MessageChatMemoryAdvisor.builder(messageWindowChatMemory)
                .build();

        return builder
                .defaultAdvisors(memoryAdvisor, ragAdvisor, new SimpleLoggerAdvisor())
                .defaultToolCallbacks(toolCallbackProvider)
                .build();
    }

    @Bean("agentGraph")
    public CompiledGraph agentGraph(ChatClient.Builder builder, VectorStore vectorStore) throws GraphStateException {
        // 策略工厂
        KeyStrategyFactory keyStrategyFactory = () -> Map.of("question", new ReplaceStrategy());
        // 创建状态图
        StateGraph stateGraph = new StateGraph("agentGraph", keyStrategyFactory);

        // 添加节点
        stateGraph.addNode("用户意图识别节点", AsyncNodeAction.node_async(new IntentRecognitionNode(builder)));
        stateGraph.addNode("产品查询节点", AsyncNodeAction.node_async(new ProductQueryNode(builder, vectorStore)));
        stateGraph.addNode("数据查询节点", AsyncNodeAction.node_async(new DataQueryNode(builder)));
        stateGraph.addNode("闲聊节点", AsyncNodeAction.node_async(new ChitChatNode(builder)));
        stateGraph.addNode("数据可视化节点", AsyncNodeAction.node_async(new DataVisualizationNode(builder)));

        // 定义基本流程 - 从START到意图识别节点
        stateGraph.addEdge(StateGraph.START, "用户意图识别节点");

        // 这里需要使用 AsyncEdgeAction.edge_async 创建条件
        stateGraph.addConditionalEdges(
                "用户意图识别节点",
                AsyncEdgeAction.edge_async(state -> {
                    Optional<UserIntent> intentOpt = state.value("userIntent", UserIntent.class);
                    UserIntent intent = intentOpt.orElse(UserIntent.CHITCHAT);
                    log.info("意图识别结果路由: {} -> {}", intent.getCode(), intent.getDescription());
                    return switch (intent) {
                        case PRODUCT_QUERY -> "产品查询节点";
                        case DATA_ASSET_QUERY -> "数据查询节点";
                        default -> "闲聊节点";
                    };
                }),
                Map.of(
                        "产品查询节点", "产品查询节点",
                        "数据查询节点", "数据查询节点",
                        "闲聊节点", "闲聊节点"
                )
        );

        stateGraph.addConditionalEdges(
                "数据查询节点",
                AsyncEdgeAction.edge_async(state -> {
                    String question = state.value("question", String.class).orElse("");
                    if (question.contains("图表") || question.contains("可视化") ||
                            question.contains("统计") || question.contains("分布")) {
                        return "数据可视化节点";
                    }
                    return StateGraph.END;
                }),
                Map.of(
                        "数据查询节点", "数据查询节点",
                        "数据可视化节点", "数据可视化节点"
                )
        );

        stateGraph.addEdge("产品查询节点", StateGraph.END);
        stateGraph.addEdge("数据可视化节点", StateGraph.END);
        stateGraph.addEdge("闲聊节点", StateGraph.END);

        printGraph(stateGraph, "agentGraph");
        return stateGraph.compile();
    }

    /**
     * 打印 Graph流程图
     */
    private void printGraph(StateGraph stateGraph, String title) {
        GraphRepresentation representation = stateGraph.getGraph(GraphRepresentation.Type.PLANTUML, title);
        log.info("\n======= expander UML FLOW ========\n");
        log.info(representation.content());
        log.info("\n============== end ===============\n");
    }
}