package com.pufferfishscheduler.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Redis消息实体
 * 用于在Redis中存储消息的临时实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisMessageEntity implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String sessionId;           // 会话ID
    private String messageId;           // 消息ID
    private String role;                // 消息角色
    private String content;              // 消息内容
    private String contentType;          // 内容类型
    private Integer tokenCount;          // Token数量
    private String modelName;            // 模型名称
    private LocalDateTime createdAt;     // 创建时间
    private Integer syncStatus;          // 同步状态：0-待同步，1-已同步
    private Integer retryCount;          // 重试次数
}