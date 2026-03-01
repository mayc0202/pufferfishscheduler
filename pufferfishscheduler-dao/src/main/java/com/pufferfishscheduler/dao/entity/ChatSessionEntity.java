package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 会话实体
 *
 * @author Mayc
 * @since 2025-12-18  14:11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("chat_session")
public class ChatSessionEntity {

    @TableId(type = IdType.AUTO)
    private Integer id;

    @TableField("session_id")
    private String sessionId;

    @TableField("session_type")
    private String sessionType;

    @TableField("user_id")
    private String userId;

    @TableField("title")
    private String title;

    @TableField("status")
    private Integer status;

    @TableField("message_count")
    private Integer messageCount;

    @TableField("token_count")
    private Integer tokenCount;

    @TableField("model_name")
    private String modelName;

    @TableField("created_at")
    private LocalDateTime createdAt;

    @TableField("updated_at")
    private LocalDateTime updatedAt;

    @TableField("deleted_at")
    private LocalDateTime deletedAt;
}
