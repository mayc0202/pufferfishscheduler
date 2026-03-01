package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 字段元数据
 *
 * @author Mayc
 * @since 2026-02-24  16:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ColumnMetadata {
    private String columnName;
    private String columnType;
    private Integer columnSize;
    private Integer decimalDigits;
    private boolean isPrimaryKey;
    private boolean isNullable;
    private String columnComment;
    private String sampleValue;
}
