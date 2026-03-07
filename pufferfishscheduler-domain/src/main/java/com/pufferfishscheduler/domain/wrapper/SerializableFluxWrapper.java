package com.pufferfishscheduler.domain.wrapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

// 创建可序列化的包装类
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SerializableFluxWrapper implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String conversationId;
    private String question;
    private String content;           // 响应内容
    private String format;             // 格式：text/markdown
    private String intentCode;          // 意图代码
    private Integer rowCount;           // 数据行数
    private Boolean hasChart;            // 是否有图表
    private Map<String, Object> chartConfig; // 图表配置
    private Boolean success;              // 是否成功
    private String errorMessage;          // 错误信息
    private Long timestamp;                // 时间戳
}
