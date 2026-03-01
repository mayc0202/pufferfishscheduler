package com.pufferfishscheduler.domain.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * ECharts 图表配置
 *
 * @author Mayc
 * @since 2026-02-25  17:32
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EChartsMetadata {

    // 图表类型
    private String type;  // bar, line, pie, scatter, etc.

    // 图表标题
    private Title title;

    // 图例
    private Legend legend;

    // 网格配置
    private Grid grid;

    // X轴配置
    private Axis xAxis;

    // Y轴配置
    private Axis yAxis;

    // 系列数据
    private List<Series> series;

    // 数据集
    private Dataset dataset;

    // 提示框
    private Tooltip tooltip;

    // 工具栏
    private Toolbox toolbox;

    // 颜色系列
    private List<String> color;

    // 是否显示为平滑曲线
    private Boolean smooth;

    // 堆叠模式
    private String stack;

    // 面积填充
    private Boolean areaStyle;

    // 其他配置
    private Map<String, Object> options;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Title {
        private String text;
        private String subtext;
        private String left;
        private String top;
        private String textStyle;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Legend {
        private Boolean show;
        private String type;
        private String orient;
        private String left;
        private String top;
        private List<String> data;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Grid {
        private String left;
        private String right;
        private String top;
        private String bottom;
        private Boolean containLabel;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Axis {
        private String type;  // category, value, time, log
        private Boolean show;
        private String position;
        private String name;
        private AxisLabel axisLabel;
        private List<String> data;
        private Boolean boundaryGap;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AxisLabel {
        private Integer rotate;
        private String formatter;
        private Boolean show;
        private Integer interval;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Series {
        private String name;
        private String type;  // bar, line, pie, etc.
        private List<Object> data;
        private String stack;
        private Boolean smooth;
        private Boolean areaStyle;
        private ItemStyle itemStyle;
        private Label label;
        private Integer barWidth;
        private String roseType;  // for pie chart
        private String center;     // for pie chart
        private String radius;     // for pie chart
        private Map<String, Object> encode;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ItemStyle {
        private String color;
        private String borderRadius;
        private Map<String, Object> emphasis;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Label {
        private Boolean show;
        private String position;
        private String formatter;
        private String color;
        private Integer fontSize;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Tooltip {
        private Boolean show;
        private String trigger;  // item, axis
        private String formatter;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Toolbox {
        private Boolean show;
        private Feature feature;

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Feature {
            private SaveAsImage saveAsImage;
            private DataView dataView;
            private Restore restore;
            private DataZoom dataZoom;
            private MagicType magicType;

            @Data
            @Builder
            @NoArgsConstructor
            @AllArgsConstructor
            public static class SaveAsImage {
                private Boolean show;
                private String type;
                private String name;
            }

            @Data
            @Builder
            @NoArgsConstructor
            @AllArgsConstructor
            public static class DataView {
                private Boolean show;
            }

            @Data
            @Builder
            @NoArgsConstructor
            @AllArgsConstructor
            public static class Restore {
                private Boolean show;
            }

            @Data
            @Builder
            @NoArgsConstructor
            @AllArgsConstructor
            public static class DataZoom {
                private Boolean show;
            }

            @Data
            @Builder
            @NoArgsConstructor
            @AllArgsConstructor
            public static class MagicType {
                private Boolean show;
                private List<String> type;
            }
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Dataset {
        private List<Map<String, Object>> source;
        private List<String> dimensions;
    }
}
