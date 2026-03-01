package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.ChatSessionEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

/**
 *
 * @author Mayc
 * @since 2025-12-18  14:23
 */
@Mapper
public interface ChatSessionMapper extends BaseMapper<ChatSessionEntity> {


    List<ChatSessionEntity> selectRecentSessions(
            @Param("sessionType") String sessionType,
            @Param("limit") int limit,
            @Param("since") LocalDateTime since);

    void incrementMessageCount(@Param("sessionId") String sessionId);

    void softDelete(@Param("sessionId") String sessionId);

    void softDeleteByType(@Param("sessionType") String sessionType);
}
