package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.ChatMessageEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

/**
 *
 * @author Mayc
 * @since 2025-12-18  14:25
 */
@Mapper
public interface ChatMessageMapper extends BaseMapper<ChatMessageEntity> {

    List<ChatMessageEntity> selectBySessionId(
            @Param("sessionId") String sessionId,
            @Param("limit") int limit);

    List<ChatMessageEntity> selectBySessionIdAndTime(
            @Param("sessionId") String sessionId,
            @Param("startTime") LocalDateTime startTime);
}
