//package com.pufferfishscheduler.ai.agent.service.impl;
//
//import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
//import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
//import com.pufferfishscheduler.dao.entity.ChatSessionEntity;
//import com.pufferfishscheduler.dao.mapper.ChatSessionMapper;
//import com.pufferfishscheduler.ai.agent.service.ChatSessionService;
//import com.pufferfishscheduler.common.exception.BusinessException;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.LocalDateTime;
//import java.util.Collections;
//import java.util.List;
//import java.util.Objects;
//
///**
// * 会话管理服务实现类
// * 负责会话的CRUD、统计更新等操作
// *
// * @author Mayc
// * @since 2026-02-22 21:35
// */
//@Slf4j
//@Service
//@RequiredArgsConstructor  // 替代@Autowired，构造器注入更安全
//public class ChatSessionServiceImpl implements ChatSessionService {
//
//    // 构造器注入（推荐方式）
//    private final ChatSessionMapper chatSessionMapper;
//
//    /**
//     * 获取最近会话列表
//     *
//     * @param sessionType 会话类型
//     * @param limit       数量限制
//     * @param since       起始时间
//     * @return 会话列表
//     */
//    @Override
//    public List<ChatSessionEntity> selectRecentSessions(String sessionType, Integer limit, LocalDateTime since) {
//        // 空值校验
//        if (sessionType == null || sessionType.isBlank()) {
//            log.warn("会话类型为空，返回空列表");
//            return Collections.emptyList();
//        }
//        if (limit == null || limit <= 0) {
//            limit = 100;  // 默认返回100条
//            log.debug("会话数量限制为空或非法，使用默认值: {}", limit);
//        }
//        if (since == null) {
//            since = LocalDateTime.now().minusDays(7);  // 默认7天内
//            log.debug("起始时间为空，使用默认值: {}", since);
//        }
//
//        try {
//            List<ChatSessionEntity> sessions = chatSessionMapper.selectRecentSessions(sessionType, limit, since);
//            log.debug("查询最近会话成功，类型: {}, 数量: {}", sessionType, CollectionUtils.isEmpty(sessions) ? 0 : sessions.size());
//            return CollectionUtils.isEmpty(sessions) ? Collections.emptyList() : sessions;
//        } catch (Exception e) {
//            log.error("查询最近会话失败，类型: {}, 限制: {}, 起始时间: {}", sessionType, limit, since, e);
//            throw new BusinessException("查询最近会话失败：" + e.getMessage());
//        }
//    }
//
//    /**
//     * 根据会话ID获取会话信息
//     *
//     * @param sessionId 会话ID
//     * @return 会话实体
//     */
//    @Override
//    public ChatSessionEntity selectBySessionId(String sessionId) {
//        // 严格的空值校验
//        if (sessionId == null || sessionId.isBlank()) {
//            log.error("会话ID为空，无法查询");
//            throw new BusinessException("会话ID不能为空");
//        }
//
//        try {
//            // 使用LambdaQueryWrapper，类型更安全
//            LambdaQueryWrapper<ChatSessionEntity> queryWrapper = new LambdaQueryWrapper<ChatSessionEntity>()
//                    .eq(ChatSessionEntity::getSessionId, sessionId)
//                    .isNull(ChatSessionEntity::getDeletedAt);
//
//            ChatSessionEntity session = chatSessionMapper.selectOne(queryWrapper);
//
//            if (Objects.isNull(session)) {
//                log.warn("会话不存在，sessionId: {}", sessionId);
//                throw new BusinessException(String.format("会话不存在，sessionId=%s", sessionId));
//            }
//
//            log.debug("查询会话成功，sessionId: {}", sessionId);
//            return session;
//        } catch (BusinessException e) {
//            throw e;  // 业务异常直接抛出
//        } catch (Exception e) {
//            log.error("查询会话失败，sessionId: {}", sessionId, e);
//            throw new BusinessException("查询会话失败：" + e.getMessage());
//        }
//    }
//
//    /**
//     * 新增会话
//     *
//     * @param sessionEntity 会话实体
//     */
//    @Transactional(rollbackFor = Exception.class)
//    @Override
//    public void insert(ChatSessionEntity sessionEntity) {
//        if (sessionEntity == null) {
//            log.error("会话实体为空，无法新增");
//            throw new BusinessException("会话实体不能为空");
//        }
//        // 补充默认值
//        if (sessionEntity.getStatus() == null) {
//            sessionEntity.setStatus(1);
//        }
//        if (sessionEntity.getCreatedAt() == null) {
//            sessionEntity.setCreatedAt(LocalDateTime.now());
//        }
//        if (sessionEntity.getUpdatedAt() == null) {
//            sessionEntity.setUpdatedAt(LocalDateTime.now());
//        }
//
//        try {
//            chatSessionMapper.insert(sessionEntity);
//            log.info("新增会话成功，sessionId: {}", sessionEntity.getSessionId());
//        } catch (Exception e) {
//            log.error("新增会话失败，sessionEntity: {}", sessionEntity, e);
//            throw new BusinessException("新增会话失败：" + e.getMessage());
//
//        }
//    }
//
//    /**
//     * 增加会话消息计数
//     *
//     * @param sessionId 会话ID
//     */
//    @Transactional(rollbackFor = Exception.class)
//    @Override
//    public void incrementMessageCount(String sessionId) {
//        if (sessionId == null || sessionId.isBlank()) {
//            log.error("会话ID为空，无法更新消息计数");
//            throw new BusinessException("会话ID不能为空");
//        }
//
//        try {
//            chatSessionMapper.incrementMessageCount(sessionId);
//            log.debug("更新会话消息计数成功，sessionId: {}", sessionId);
//        } catch (Exception e) {
//            log.error("更新会话消息计数失败，sessionId: {}", sessionId, e);
//            throw new BusinessException("更新会话消息计数失败：" + e.getMessage());
//        }
//    }
//
//    /**
//     * 根据ID更新会话
//     *
//     * @param sessionEntity 会话实体
//     */
//    @Transactional(rollbackFor = Exception.class)
//    @Override
//    public void updateById(ChatSessionEntity sessionEntity) {
//        if (sessionEntity == null || sessionEntity.getId() == null) {
//            log.error("会话实体或ID为空，无法更新");
//            throw new BusinessException("会话实体和ID不能为空");
//        }
//        // 自动更新时间
//        sessionEntity.setUpdatedAt(LocalDateTime.now());
//
//        try {
//            chatSessionMapper.updateById(sessionEntity);
//            log.debug("更新会话成功，ID: {}", sessionEntity.getId());
//        } catch (Exception e) {
//            log.error("更新会话失败，sessionEntity: {}", sessionEntity, e);
//            throw new BusinessException("更新会话失败：" + e.getMessage());
//        }
//    }
//
//    /**
//     * 扩展方法：根据类型获取所有会话ID（解耦用）
//     *
//     * @param sessionType 会话类型
//     * @return 会话ID列表
//     */
//    @Override
//    public List<String> getSessionIdsByType(String sessionType) {
//        List<ChatSessionEntity> sessions = selectRecentSessions(sessionType, 1000, LocalDateTime.now().minusDays(30));
//        return sessions.stream()
//                .map(ChatSessionEntity::getSessionId)
//                .toList();
//    }
//}