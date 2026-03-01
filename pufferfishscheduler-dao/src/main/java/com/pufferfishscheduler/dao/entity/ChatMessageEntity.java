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
 * 消息实体
 *
 * @author Mayc
 * @since 2025-12-18  14:13
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("chat_message")
public class ChatMessageEntity {

    @TableId(type = IdType.AUTO)
    private Integer id;

    @TableField("session_id")
    private String sessionId;

    @TableField("message_id")
    private String messageId;

    @TableField("parent_message_id")
    private String parentMessageId;

    @TableField("role")
    private String role;

    @TableField("content_type")
    private String contentType;

    @TableField("content")
    private String content;

    @TableField("token_count")
    private Integer tokenCount;

    @TableField("model_name")
    private String modelName;

    @TableField("metadata")
    private String metadata;

    @TableField("created_at")
    private LocalDateTime createdAt;
}
