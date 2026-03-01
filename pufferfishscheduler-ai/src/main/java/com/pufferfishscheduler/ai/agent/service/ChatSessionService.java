//package com.pufferfishscheduler.ai.agent.service;
//
//
//
//import com.pufferfishscheduler.dao.entity.ChatSessionEntity;
//
//import java.time.LocalDateTime;
//import java.util.List;
//
///**
// *
// * @author Mayc
// * @since 2026-02-22  20:52
// */
//public interface ChatSessionService {
//    /**
//     * 获取最近会话列表
//     *
//     * @param sessionType
//     * @param limit
//     * @param since
//     * @return
//     */
//    List<ChatSessionEntity> selectRecentSessions(String sessionType, Integer limit, LocalDateTime since);
//
//    List<String> getSessionIdsByType(String sessionType);
//
//    /**
//     * 通过会话id查询会话记录
//     *
//     * @param sessionId
//     * @return
//     */
//    ChatSessionEntity selectBySessionId(String sessionId);
//
//    void insert(ChatSessionEntity chatSessionEntity);
//
//    void incrementMessageCount(String sessionId);
//
//    void updateById(ChatSessionEntity chatSessionEntity);
//}
