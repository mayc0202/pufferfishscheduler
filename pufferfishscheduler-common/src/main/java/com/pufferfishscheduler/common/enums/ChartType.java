package com.pufferfishscheduler.common.enums;

import lombok.Getter;

/**
 * 图表类型枚举
 *
 * @author Mayc
 * @since 2026-02-25  17:36
 */
@Getter
public enum ChartType {
    BAR("bar", "柱状图"),
    LINE("line", "折线图"),
    PIE("pie", "饼图"),
    SCATTER("scatter", "散点图"),
    RADAR("radar", "雷达图"),
    FUNNEL("funnel", "漏斗图"),
    GAUGE("gauge", "仪表盘"),
    CANDLESTICK("candlestick", "K线图"),
    HEATMAP("heatmap", "热力图"),
    TREE("tree", "树图"),
    TREEMAP("treemap", "矩形树图"),
    SUNBURST("sunburst", "旭日图"),
    SANKEY("sankey", "桑基图"),
    GRAPH("graph", "关系图");

    private final String code;
    private final String description;

    ChartType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static ChartType fromCode(String code) {
        for (ChartType type : ChartType.values()) {
            if (type.getCode().equals(code)) {
                return type;
            }
        }
        return BAR; // 默认返回柱状图
    }
}
