package com.pufferfishscheduler.ai.engine.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pufferfishscheduler.common.enums.ChartType;
import com.pufferfishscheduler.common.result.ChartAnalysis;
import com.pufferfishscheduler.domain.domain.EChartsMetaData;
import com.pufferfishscheduler.domain.domain.QueryResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 * @author Mayc
 * @since 2026-02-25  17:37
 */
@Slf4j
public class DataVisualizationNode implements NodeAction {

    private static final String CHART_ANALYSIS_PROMPT = """
            你是一个数据可视化专家，需要分析用户的问题和查询结果，推荐合适的图表类型并生成ECharts配置。
            
            用户问题：{question}
            
            查询结果：共 {rowCount} 条记录
            
            数据样例（前3条）：
            {sampleData}
            
            字段信息：
            {columnInfo}
            
            请分析数据特点并返回JSON格式的图表配置，包含以下字段：
            1. chartType: 推荐的图表类型（bar/line/pie/scatter/radar等）
            2. title: 图表标题
            3. xAxisColumn: X轴使用的字段名
            4. yAxisColumns: Y轴使用的字段名列表
            5. seriesNames: 系列名称列表
            6. description: 图表描述
            
            只返回JSON格式，不要有其他解释。
            
            例如：
            {
                "chartType": "bar",
                "title": "各类型数据库数量统计",
                "xAxisColumn": "type",
                "yAxisColumns": ["count"],
                "seriesNames": ["数量"],
                "description": "显示不同类型数据库的分布情况"
            }
            """;

    private final ChatClient chatClient;
    private final ObjectMapper objectMapper;

    public DataVisualizationNode(ChatClient.Builder builder) {
        this.chatClient = builder.build();

        this.objectMapper = new ObjectMapper();
        // 注册Java 8时间模块
        objectMapper.registerModule(new JavaTimeModule());
        // 配置时间格式
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // 其他配置
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Map<String, Object> apply(OverAllState state) throws Exception {
        Optional<QueryResult> queryResultOpt = getValueAsType(state, "queryResult", QueryResult.class);

        if (queryResultOpt.isEmpty()) {
            log.warn("没有找到查询结果");
            return Map.of(
                    "error", "没有可用的查询数据",
                    "success", false
            );
        }

        QueryResult queryResult = queryResultOpt.get();

        String question = state.value("question", String.class).orElse("");

        if (queryResult.getData() == null || queryResult.getData().isEmpty()) {
            return Map.of(
                    "error", "查询结果为空",
                    "success", false
            );
        }

        log.info("开始数据可视化分析，数据行数：{}", queryResult.getRowCount());

        try {
            // 1. 分析数据并推荐图表类型
            ChartAnalysis analysis = analyzeData(question, queryResult);

            // 2. 根据推荐生成 ECharts 配置
            EChartsMetaData chartConfig = generateEChartsMetadata(analysis, queryResult);

            // 3. 返回图表配置和数据
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("chartType", analysis.getChartType());
            result.put("chartConfig", chartConfig);
            result.put("rowCount", queryResult.getRowCount());
            result.put("analysis", analysis.getDescription());

            // 同时保留原始数据供前端使用
            result.put("chartData", queryResult.getData());

            // 生成友好的回答
            String response = generateVisualizationResponse(analysis, queryResult);
            result.put("response", response);

            return result;

        } catch (Exception e) {
            log.error("数据可视化失败", e);
            return generateFallbackChart(queryResult);
        }
    }

    /**
     * 类型安全的从 OverAllState 获取值
     */
    private <T> Optional<T> getValueAsType(OverAllState state, String key, Class<T> type) {
        try {
            return state.value(key, type);
        } catch (Exception e) {
            log.debug("无法将 key '{}' 转换为类型 {}: {}", key, type.getSimpleName(), e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 生成可视化的文字回答
     */
    private String generateVisualizationResponse(ChartAnalysis analysis, QueryResult queryResult) {
        return String.format(
                "已为您生成%s图表，展示%s，共%d条数据。",
                analysis.getChartType(),
                analysis.getTitle(),
                queryResult.getRowCount()
        );
    }

    /**
     * 分析数据并推荐图表类型
     */
    private ChartAnalysis analyzeData(String question, QueryResult queryResult) throws Exception {
        // 准备数据样例
        List<Map<String, Object>> data = queryResult.getData();
        List<Map<String, Object>> sampleData = data.stream().limit(3).collect(Collectors.toList());

        // 获取字段信息
        Set<String> columns = data.get(0).keySet();
        String columnInfo = columns.stream()
                .map(col -> {
                    Object value = data.get(0).get(col);
                    String type = value != null ? value.getClass().getSimpleName() : "unknown";
                    return col + "(" + type + ")";
                })
                .collect(Collectors.joining(", "));

        // 使用 LLM 分析数据
        PromptTemplate template = new PromptTemplate(CHART_ANALYSIS_PROMPT);
        String prompt = template.render(Map.of(
                "question", question,
                "rowCount", queryResult.getRowCount(),
                "sampleData", objectMapper.writeValueAsString(sampleData),
                "columnInfo", columnInfo
        ));

        String result = chatClient.prompt()
                .user(prompt)
                .call()
                .content();

        return objectMapper.readValue(result, ChartAnalysis.class);
    }

    /**
     * 生成 ECharts 配置
     */
    private EChartsMetaData generateEChartsMetadata(ChartAnalysis analysis, QueryResult queryResult) {
        List<Map<String, Object>> data = queryResult.getData();
        ChartType chartType = ChartType.fromCode(analysis.getChartType());

        EChartsMetaData.EChartsMetaDataBuilder builder = EChartsMetaData.builder()
                .type(analysis.getChartType())
                .title(EChartsMetaData.Title.builder()
                        .text(analysis.getTitle())
                        .left("center")
                        .build())
                .tooltip(EChartsMetaData.Tooltip.builder()
                        .show(true)
                        .trigger(chartType == ChartType.PIE ? "item" : "axis")
                        .build())
                .toolbox(EChartsMetaData.Toolbox.builder()
                        .show(true)
                        .feature(EChartsMetaData.Toolbox.Feature.builder()
                                .saveAsImage(EChartsMetaData.Toolbox.Feature.SaveAsImage.builder()
                                        .show(true)
                                        .type("png")
                                        .build())
                                .dataView(EChartsMetaData.Toolbox.Feature.DataView.builder()
                                        .show(true)
                                        .build())
                                .restore(EChartsMetaData.Toolbox.Feature.Restore.builder()
                                        .show(true)
                                        .build())
                                .build())
                        .build());

        switch (chartType) {
            case BAR:
            case LINE:
                builder = buildBarOrLineChart(builder, analysis, data, chartType);
                break;
            case PIE:
                builder = buildPieChart(builder, analysis, data);
                break;
            case SCATTER:
                builder = buildScatterChart(builder, analysis, data);
                break;
            default:
                builder = buildDefaultChart(builder, analysis, data);
        }

        return builder.build();
    }

    /**
     * 构建柱状图/折线图
     */
    private EChartsMetaData.EChartsMetaDataBuilder buildBarOrLineChart(
            EChartsMetaData.EChartsMetaDataBuilder builder,
            ChartAnalysis analysis,
            List<Map<String, Object>> data,
            ChartType chartType) {

        String xAxisColumn = analysis.getXAxisColumn();
        List<String> yAxisColumns = analysis.getYAxisColumns();
        List<String> seriesNames = analysis.getSeriesNames();

        // 提取 X 轴数据
        List<String> xAxisData = data.stream()
                .map(row -> String.valueOf(row.get(xAxisColumn)))
                .collect(Collectors.toList());

        // 配置 X 轴
        builder.xAxis(EChartsMetaData.Axis.builder()
                .type("category")
                .data(xAxisData)
                .axisLabel(EChartsMetaData.AxisLabel.builder()
                        .rotate(xAxisData.size() > 10 ? 45 : 0)
                        .build())
                .build());

        // 配置 Y 轴
        builder.yAxis(EChartsMetaData.Axis.builder()
                .type("value")
                .build());

        // 配置系列
        List<EChartsMetaData.Series> seriesList = new ArrayList<>();
        for (int i = 0; i < yAxisColumns.size(); i++) {
            String column = yAxisColumns.get(i);
            String seriesName = i < seriesNames.size() ? seriesNames.get(i) : column;

            List<Object> seriesData = data.stream()
                    .map(row -> row.get(column))
                    .collect(Collectors.toList());

            EChartsMetaData.Series series = EChartsMetaData.Series.builder()
                    .name(seriesName)
                    .type(chartType.getCode())
                    .data(seriesData)
                    .build();

            seriesList.add(series);
        }
        builder.series(seriesList);

        return builder;
    }

    /**
     * 构建饼图
     */
    private EChartsMetaData.EChartsMetaDataBuilder buildPieChart(
            EChartsMetaData.EChartsMetaDataBuilder builder,
            ChartAnalysis analysis,
            List<Map<String, Object>> data) {

        String nameColumn = analysis.getXAxisColumn();
        String valueColumn = analysis.getYAxisColumns().get(0);

        // 配置饼图数据
        List<Map<String, Object>> pieData = data.stream()
                .map(row -> {
                    Map<String, Object> item = new HashMap<>();
                    item.put("name", row.get(nameColumn));
                    item.put("value", row.get(valueColumn));
                    return item;
                })
                .collect(Collectors.toList());

        // 创建数据集
        builder.dataset(EChartsMetaData.Dataset.builder()
                .source(pieData)
                .build());

        // 配置系列
        EChartsMetaData.Series series = EChartsMetaData.Series.builder()
                .name(analysis.getTitle())
                .type("pie")
                .radius("55%")
                .center("50%, 50%")
                .encode(Map.of("itemName", "name", "value", "value"))
                .build();

        builder.series(List.of(series));

        return builder;
    }

    /**
     * 构建散点图
     */
    private EChartsMetaData.EChartsMetaDataBuilder buildScatterChart(
            EChartsMetaData.EChartsMetaDataBuilder builder,
            ChartAnalysis analysis,
            List<Map<String, Object>> data) {

        List<String> yAxisColumns = analysis.getYAxisColumns();

        // 配置 X 轴
        builder.xAxis(EChartsMetaData.Axis.builder()
                .type("value")
                .name(analysis.getXAxisColumn())
                .build());

        // 配置 Y 轴
        builder.yAxis(EChartsMetaData.Axis.builder()
                .type("value")
                .name(yAxisColumns.get(0))
                .build());

        // 配置系列
        List<EChartsMetaData.Series> seriesList = new ArrayList<>();
        for (String column : yAxisColumns) {
            List<Object> seriesData = data.stream()
                    .map(row -> Arrays.asList(row.get(analysis.getXAxisColumn()), row.get(column)))
                    .collect(Collectors.toList());

            EChartsMetaData.Series series = EChartsMetaData.Series.builder()
                    .name(column)
                    .type("scatter")
                    .data(seriesData)
                    .build();

            seriesList.add(series);
        }
        builder.series(seriesList);

        return builder;
    }

    /**
     * 构建默认图表
     */
    private EChartsMetaData.EChartsMetaDataBuilder buildDefaultChart(
            EChartsMetaData.EChartsMetaDataBuilder builder,
            ChartAnalysis analysis,
            List<Map<String, Object>> data) {

        // 如果没有分析结果，使用第一个数值字段创建简单柱状图
        String firstColumn = data.get(0).keySet().iterator().next();
        List<String> xAxisData = new ArrayList<>();
        List<Object> seriesData = new ArrayList<>();

        for (int i = 0; i < Math.min(data.size(), 20); i++) {
            Map<String, Object> row = data.get(i);
            xAxisData.add("项" + (i + 1));
            seriesData.add(row.values().iterator().next());
        }

        builder.xAxis(EChartsMetaData.Axis.builder()
                .type("category")
                .data(xAxisData)
                .build());

        builder.yAxis(EChartsMetaData.Axis.builder()
                .type("value")
                .build());

        builder.series(List.of(EChartsMetaData.Series.builder()
                .name("数据")
                .type("bar")
                .data(seriesData)
                .build()));

        return builder;
    }

    /**
     * 降级方案：自动生成简单图表
     */
    private Map<String, Object> generateFallbackChart(QueryResult queryResult) {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("chartType", "bar");
        result.put("analysis", "自动生成的基础图表");

        // 尝试识别数值字段
        List<Map<String, Object>> data = queryResult.getData();
        if (data == null || data.isEmpty()) {
            return result;
        }

        // 找到第一个数值字段
        String valueField = null;
        String categoryField = null;
        Map<String, Object> firstRow = data.get(0);

        for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
            if (entry.getValue() instanceof Number) {
                valueField = entry.getKey();
            } else if (entry.getValue() instanceof String) {
                categoryField = entry.getKey();
            }
        }

        if (valueField == null) {
            valueField = firstRow.keySet().iterator().next();
        }

        // 构建简单图表配置
        EChartsMetaData.EChartsMetaDataBuilder builder = EChartsMetaData.builder()
                .type("bar")
                .title(EChartsMetaData.Title.builder()
                        .text("数据可视化")
                        .left("center")
                        .build());

        if (categoryField != null) {
            String finalCategoryField = categoryField;
            List<String> xAxisData = data.stream()
                    .limit(20)
                    .map(row -> String.valueOf(row.get(finalCategoryField)))
                    .collect(Collectors.toList());

            builder.xAxis(EChartsMetaData.Axis.builder()
                    .type("category")
                    .data(xAxisData)
                    .build());
        }

        String finalValueField = valueField;
        List<Object> seriesData = data.stream()
                .limit(20)
                .map(row -> row.get(finalValueField))
                .collect(Collectors.toList());

        builder.yAxis(EChartsMetaData.Axis.builder()
                .type("value")
                .build());

        builder.series(List.of(EChartsMetaData.Series.builder()
                .name(valueField)
                .type("bar")
                .data(seriesData)
                .build()));

        result.put("chartConfig", builder.build());
        result.put("chartData", data);
        result.put("rowCount", queryResult.getRowCount());

        return result;
    }
    
}
