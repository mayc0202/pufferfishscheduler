package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 表元数据
 *
 * @author Mayc
 * @since 2026-02-24  16:26
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TableMetaData {
    private String tableName;
    private String tableComment;
    private List<ColumnMetaData> columns;
    private Map<String, Object> sampleData;
    private Integer rowCount; // 可选的：总行数
}
