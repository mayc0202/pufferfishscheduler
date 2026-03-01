package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 查询结果
 *
 * @author Mayc
 * @since 2026-02-24  16:28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class QueryResult {
    private String sql;
    private List<Map<String, Object>> data;
    private int rowCount;
    private long executionTime;
    private String errorMessage;
    private boolean success;
    private Map<String, Object> summary; // 可选的：统计信息
}
