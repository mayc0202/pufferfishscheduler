package com.pufferfishscheduler.common.result;

import lombok.Data;

import java.util.List;

/**
 * 图表分析结果类
 *
 * @author Mayc
 * @since 2026-02-25  17:41
 */
@Data
public class ChartAnalysis {
    private String chartType;
    private String title;
    private String xAxisColumn;
    private List<String> yAxisColumns;
    private List<String> seriesNames;
    private String description;
}
