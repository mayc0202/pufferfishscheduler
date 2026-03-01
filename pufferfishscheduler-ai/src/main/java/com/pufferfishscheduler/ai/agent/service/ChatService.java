//package com.pufferfishscheduler.ai.agent.service;
//
//import com.pufferfishscheduler.ai.agent.vo.MessageVo;
//import reactor.core.publisher.Flux;
//
//import java.util.List;
//
///**
// *
// * @author Mayc
// * @since 2026-02-22  12:22
// */
//public interface ChatService {
//
//    /**
//     * 流式询问
//     * @param conversationId 会话id
//     * @param prompt 提示词
//     * @return
//     */
//    Flux<String> ask(String conversationId, String prompt);
//
//    /**
//     * 保存会话记录
//     *
//     * @param type
//     * @param conversationId
//     */
//    void addSessionId(String type, String conversationId);
//
//    /**
//     * 获取会话ID列表
//     *
//     * @param type
//     * @return
//     */
//    List<String> getSessionIds(String type);
//
//    /**
//     * 获取会话历史
//     *
//     * @param type
//     * @param conversationId
//     * @return
//     */
//    List<MessageVo> getSessionHistory(String type, String conversationId);
//}
