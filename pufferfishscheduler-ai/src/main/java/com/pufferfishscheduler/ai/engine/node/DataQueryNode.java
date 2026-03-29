package com.pufferfishscheduler.ai.engine.node;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.domain.QueryResult;
import com.pufferfishscheduler.domain.domain.TableMetadata;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 数据查询节点 - 支持动态数据源
 *
 * @author Mayc
 * @since 2026-02-24 14:02
 */
@Slf4j
public class DataQueryNode implements NodeAction {

    private static final String SQL_GENERATION_PROMPT = """
            你是一个SQL专家，需要根据用户的问题和数据库元数据生成合适的SQL查询语句。
            
            用户问题：{question}
            
            可用的表结构信息：
            {tableMetadata}
            
            要求：
            1. 只生成SELECT查询语句，不要生成INSERT、UPDATE、DELETE语句
            2. SQL语句要安全，防止SQL注入
            3. 如果用户问题不明确，返回合适的提示信息
            4. 只返回SQL语句，不要有其他解释
            
            请生成SQL：""";

    private static final String RESPONSE_FORMAT_PROMPT = """
            你是一个数据分析助手，需要将SQL查询结果转换成友好的自然语言回答。
            
            用户问题：{question}
            
            执行的SQL：{sql}
            
            查询结果：
            {queryResult}
            
            请用友好的语气回答用户的问题，基于查询结果给出准确的信息：""";

    // 使用 %s 而不是 {question} 避免ST模板解析错误
    private static final String DATA_SOURCE_EXTRACT_PROMPT = """
            请从用户的问题中提取数据库连接信息，以JSON格式返回。
            
            用户问题：%s
            
            需要提取的信息包括：
            - type: 数据库类型（mysql/oracle/postgresql/sqlserver）
            - host: 数据库主机地址
            - port: 端口号
            - database: 数据库名称
            - username: 用户名
            - password: 密码
            - schema: 模式名称（可选）
            
            如果用户没有提供完整的连接信息，缺失的字段用null表示。
            只返回JSON格式，不要有其他解释。
            
            例如：
            {
                "type": "mysql",
                "host": "localhost",
                "port": 3306,
                "database": "test_db",
                "username": "root",
                "password": "123456",
                "schema": null
            }
            
            JSON结果：""";

    private final ChatClient chatClient;
    private final ObjectMapper objectMapper;

    public DataQueryNode(ChatClient.Builder builder) {
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
    public Map<String, Object> apply(OverAllState state) {
        String question = Optional.ofNullable(state.value("question"))
                .map(Object::toString)
                .orElse("");

        log.info("开始数据查询，用户问题：{}", question);

        Connection connection = null;
        try {
            // 1. 从问题中提取数据源信息
            DataSourceInfo dataSourceInfo = extractDataSourceInfo(question);
            log.info("提取到的数据源信息：{}", dataSourceInfo);

            // 2. 检查数据源信息完整性
            if (dataSourceInfo.isIncomplete()) {
                String missingFields = dataSourceInfo.getMissingFields();
                return Map.of(
                        "response", "数据源信息不完整，缺少以下信息：" + missingFields + "。请提供完整的数据库连接信息。",
                        "queryType", "data_query",
                        "success", false
                );
            }

            // 3. 获取数据库连接
            DatabaseConnectionInfo connectionInfo = buildDatabaseMetadata(dataSourceInfo);
            String driverName = JdbcUrlUtil.getDriver(connectionInfo.getType());
            String url = JdbcUrlUtil.getUrl(
                    connectionInfo.getType(),
                    connectionInfo.getDbHost(),
                    connectionInfo.getDbPort(),
                    connectionInfo.getDbName(),
                    connectionInfo.getExtConfig()
            );

            connection = JdbcUtil.getConnection(driverName, url, connectionInfo);

            // 4. 从问题中提取要查询的表名 - 重要：不要使用 allowedTables
            List<String> tablesToQuery = extractTablesFromQuestion(question);

            if (tablesToQuery == null || tablesToQuery.isEmpty()) {
                return Map.of(
                        "response", "未指定要查询的表，请在问题中指明要查询的表名（例如：查询xxx表）。",
                        "queryType", "data_query",
                        "success", false
                );
            }

            log.info("要查询的表: {}", tablesToQuery);

            // 5. 获取表元数据
            List<TableMetadata> metadataList = JdbcUtil.getTablesMetadata(connection, tablesToQuery);

            if (metadataList.isEmpty()) {
                return Map.of(
                        "response", "未找到可查询的表，请检查表名是否正确。要查询的表: " + tablesToQuery,
                        "queryType", "data_query",
                        "success", false
                );
            }

            String metadataJson = objectMapper.writeValueAsString(metadataList);

            // 6. 生成SQL
            String sql = generateSql(question, metadataJson);
            log.info("生成的SQL：{}", sql);

            if (sql == null || sql.trim().isEmpty()) {
                return buildErrorResponse("抱歉，无法根据您的问题生成合适的SQL查询。");
            }

            // 7. 安全检查 - 使用改进的方法
            if (!isSqlSafe(sql, tablesToQuery)) {
                return buildErrorResponse("生成的SQL语句不安全，已拒绝执行。", sql);
            }

            // 8. 执行SQL
            QueryResult queryResult = JdbcUtil.executeQuery(connection, sql);
            log.info("SQL执行结果：{}条记录，耗时：{}ms",
                    queryResult.getRowCount(), queryResult.getExecutionTime());

            if (!queryResult.isSuccess()) {
                return buildErrorResponse("执行SQL时出错：" + queryResult.getErrorMessage(), sql);
            }

            // 9. 生成最终回答
            String answer = generateAnswerWithTimeout(question, sql, queryResult);

            // 10. 返回结果
            return buildSuccessResponse(answer, sql, queryResult, dataSourceInfo);

        } catch (Exception e) {
            log.error("数据查询失败", e);
            return buildErrorResponse("数据查询失败：" + e.getMessage());
        } finally {
            JdbcUtil.close(connection);
        }
    }

    /**
     * 从问题中提取数据源信息
     */
    private DataSourceInfo extractDataSourceInfo(String question) {
        DataSourceInfo info = new DataSourceInfo();

        try {
            // 优先使用正则表达式提取
            extractWithRegex(question, info);

            // 设置默认端口
            if (info.getPort() == null && info.getType() != null) {
                info.setPort(getDefaultPort(info.getType()));
            }

            // 如果正则提取不完整，尝试使用LLM
            if (info.isIncomplete()) {
                try {
                    String extracted = extractWithLLM(question);
                    if (extracted != null && !extracted.trim().isEmpty()) {
                        mergeLLMResult(extracted, info);
                    }
                } catch (Exception e) {
                    log.warn("LLM提取数据源信息失败，使用正则提取结果", e);
                }
            }

            // 注意：这里不要设置 allowedTables，让 extractTablesFromQuestion 单独处理

        } catch (Exception e) {
            log.warn("提取数据源信息失败", e);
        }

        return info;
    }

    /**
     * 从问题中提取要查询的表名 - 改进版
     */
    private List<String> extractTablesFromQuestion(String question) {
        List<String> tables = new ArrayList<>();

        // 排除的关键词（数据库类型等）
        Set<String> excludedKeywords = new HashSet<>(Arrays.asList(
                "mysql", "oracle", "postgresql", "sqlserver", "database", "数据",
                "数据库", "连接", "信息", "主机", "端口", "用户名", "密码"
        ));

        // 模式1：查询xxx表
        Pattern pattern1 = Pattern.compile("查询\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*表", Pattern.CASE_INSENSITIVE);
        Matcher matcher1 = pattern1.matcher(question);
        if (matcher1.find()) {
            String tableName = matcher1.group(1).toLowerCase();
            if (!excludedKeywords.contains(tableName)) {
                tables.add(matcher1.group(1));
            }
        }

        // 模式2：xxx表中
        Pattern pattern2 = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*表中", Pattern.CASE_INSENSITIVE);
        Matcher matcher2 = pattern2.matcher(question);
        if (matcher2.find() && tables.isEmpty()) {
            String tableName = matcher2.group(1).toLowerCase();
            if (!excludedKeywords.contains(tableName)) {
                tables.add(matcher2.group(1));
            }
        }

        // 模式3：table xxx
        Pattern pattern3 = Pattern.compile("table\\s+([a-zA-Z_][a-zA-Z0-9_]*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher3 = pattern3.matcher(question);
        if (matcher3.find() && tables.isEmpty()) {
            String tableName = matcher3.group(1).toLowerCase();
            if (!excludedKeywords.contains(tableName)) {
                tables.add(matcher3.group(1));
            }
        }

        log.info("从问题中提取到的表名: {}", tables);
        return tables;
    }

    /**
     * 合并LLM提取的结果
     */
    private void mergeLLMResult(String extracted, DataSourceInfo info) {
        try {
            Map<String, Object> extractedMap = objectMapper.readValue(extracted, new TypeReference<Map<String, Object>>() {});

            if (info.getType() == null) info.setType((String) extractedMap.get("type"));
            if (info.getHost() == null) info.setHost((String) extractedMap.get("host"));
            if (info.getPort() == null && extractedMap.get("port") != null) {
                info.setPort(Integer.parseInt(extractedMap.get("port").toString()));
            }
            if (info.getDatabase() == null) info.setDatabase((String) extractedMap.get("database"));
            if (info.getUsername() == null) info.setUsername((String) extractedMap.get("username"));
            if (info.getPassword() == null) info.setPassword((String) extractedMap.get("password"));
            if (info.getSchema() == null) info.setSchema((String) extractedMap.get("schema"));

        } catch (Exception e) {
            log.warn("解析LLM提取结果失败", e);
        }
    }

    /**
     * 使用LLM提取数据源信息
     */
    private String extractWithLLM(String question) {
        try {
            String prompt = String.format(DATA_SOURCE_EXTRACT_PROMPT, question);

            return chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();
        } catch (Exception e) {
            log.warn("LLM提取数据源信息失败", e);
            return null;
        }
    }

    /**
     * 使用正则表达式提取数据源信息
     */
    private void extractWithRegex(String question, DataSourceInfo info) {
        // 提取数据库类型
        Pattern typePattern = Pattern.compile("(?:数据库类型|type)[：:\\s]*([a-z]+)", Pattern.CASE_INSENSITIVE);
        Matcher typeMatcher = typePattern.matcher(question);
        if (typeMatcher.find()) {
            info.setType(typeMatcher.group(1).toLowerCase());
        }

        // 提取主机地址
        Pattern hostPattern = Pattern.compile("(?:主机|host|地址|ip)[：:\\s]*([\\d\\.]+|[\\w\\.-]+)", Pattern.CASE_INSENSITIVE);
        Matcher hostMatcher = hostPattern.matcher(question);
        if (hostMatcher.find()) {
            info.setHost(hostMatcher.group(1).trim());
        }

        // 提取端口
        Pattern portPattern = Pattern.compile("(?:端口|port)[：:\\s]*(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher portMatcher = portPattern.matcher(question);
        if (portMatcher.find()) {
            info.setPort(Integer.parseInt(portMatcher.group(1)));
        }

        // 提取数据库名
        Pattern dbPattern = Pattern.compile("(?:数据库|database|db)[：:\\s]*([\\w_-]+)", Pattern.CASE_INSENSITIVE);
        Matcher dbMatcher = dbPattern.matcher(question);
        if (dbMatcher.find()) {
            info.setDatabase(dbMatcher.group(1));
        }

        // 提取用户名
        Pattern userPattern = Pattern.compile("(?:用户名|user|username)[：:\\s]*([\\w_-]+)", Pattern.CASE_INSENSITIVE);
        Matcher userMatcher = userPattern.matcher(question);
        if (userMatcher.find()) {
            info.setUsername(userMatcher.group(1));
        }

        // 提取密码
        Pattern pwdPattern = Pattern.compile("(?:密码|password|pwd)[：:\\s]*([\\w!@#$%^&*()_+=-]+)", Pattern.CASE_INSENSITIVE);
        Matcher pwdMatcher = pwdPattern.matcher(question);
        if (pwdMatcher.find()) {
            info.setPassword(pwdMatcher.group(1));
        }
    }

    /**
     * 获取默认端口
     */
    private Integer getDefaultPort(String dbType) {
        if (dbType == null) return null;

        switch (dbType.toLowerCase()) {
            case "mysql": return 3306;
            case "oracle": return 1521;
            case "postgresql": return 5432;
            case "sqlserver": return 1433;
            case "kingbase": return 54321;
            case "gaussdb": return 8000;
            default: return null;
        }
    }

    /**
     * 构建DatabaseMetadata对象
     */
    private DatabaseConnectionInfo buildDatabaseMetadata(DataSourceInfo info) {
        if (info.isIncomplete()) {
            throw new IllegalArgumentException("数据源信息不完整: " + info);
        }

        return DatabaseConnectionInfo.builder()
                .type(info.getType())
                .dbHost(info.getHost())
                .dbPort(String.valueOf(info.getPort()))
                .dbName(info.getDatabase())
                .username(info.getUsername())
                .password(info.getPassword())
                .dbSchema(info.getSchema())
                .build();
    }

    /**
     * 生成SQL
     */
    private String generateSql(String question, String metadataJson) {
        PromptTemplate template = new PromptTemplate(SQL_GENERATION_PROMPT);
        String prompt = template.render(Map.of(
                "question", question,
                "tableMetadata", metadataJson
        ));

        String sql = chatClient.prompt()
                .user(prompt)
                .call()
                .content();

        return sql != null ? sql.trim() : null;
    }

    /**
     * SQL安全检查 - 改进版，避免字段名误判
     */
    private boolean isSqlSafe(String sql, List<String> allowedTables) {
        if (sql == null) return false;

        String upperSql = sql.toUpperCase();

        // 1. 检查是否是SELECT语句
        if (!upperSql.trim().startsWith("SELECT")) {
            log.warn("SQL不是SELECT语句：{}", sql);
            return false;
        }

        // 2. 检查禁止的操作 - 使用单词边界确保是独立的关键字
        Map<String, String> forbiddenPatterns = new LinkedHashMap<>();
        forbiddenPatterns.put("INSERT", "\\bINSERT\\b");
        forbiddenPatterns.put("UPDATE", "\\bUPDATE\\b");
        forbiddenPatterns.put("DELETE", "\\bDELETE\\b");
        forbiddenPatterns.put("DROP", "\\bDROP\\b");
        forbiddenPatterns.put("TRUNCATE", "\\bTRUNCATE\\b");
        forbiddenPatterns.put("ALTER", "\\bALTER\\b");
        forbiddenPatterns.put("CREATE", "\\bCREATE\\b");
        forbiddenPatterns.put("GRANT", "\\bGRANT\\b");
        forbiddenPatterns.put("REVOKE", "\\bREVOKE\\b");

        for (Map.Entry<String, String> entry : forbiddenPatterns.entrySet()) {
            Pattern pattern = Pattern.compile(entry.getValue(), Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                log.warn("SQL包含禁止的操作：{}，位置：{}", entry.getKey(), matcher.start());
                return false;
            }
        }

        // 3. 检查是否查询了允许的表
        boolean foundAllowedTable = false;
        for (String table : allowedTables) {
            // 使用单词边界确保是独立的表名
            String tablePattern = "\\b" + Pattern.quote(table) + "\\b";
            Pattern pattern = Pattern.compile(tablePattern, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                foundAllowedTable = true;
                log.debug("找到允许的表: {}", table);
                break;
            }
        }

        if (!foundAllowedTable) {
            log.warn("SQL没有查询任何允许的表，允许的表：{}，SQL：{}", allowedTables, sql);
            return false;
        }

        return true;
    }

    /**
     * 生成回答
     */
    private String generateAnswer(String question, String sql, QueryResult queryResult) {
        try {
            String resultJson = objectMapper.writeValueAsString(queryResult);

            PromptTemplate template = new PromptTemplate(RESPONSE_FORMAT_PROMPT);
            String prompt = template.render(Map.of(
                    "question", question,
                    "sql", sql,
                    "queryResult", resultJson
            ));

            return chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

        } catch (Exception e) {
            log.error("生成回答失败", e);

            // 降级返回简单格式
            StringBuilder sb = new StringBuilder();
            sb.append("查询到").append(queryResult.getRowCount()).append("条记录：\n");

            if (queryResult.getData() != null) {
                for (Map<String, Object> row : queryResult.getData()) {
                    sb.append(row.toString()).append("\n");
                }
            }

            return sb.toString();
        }
    }

    /**
     * 生成回答 - 带超时控制
     */
    private String generateAnswerWithTimeout(String question, String sql, QueryResult queryResult) {
        // 如果结果太大，简化输出
        if (queryResult.getRowCount() > 100) {
            return generateSimpleAnswer(question, sql, queryResult);
        }

        // 尝试生成自然语言回答，但设置超时
        try {
            // 使用异步调用
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return generateAnswer(question, sql, queryResult);
                } catch (Exception e) {
                    log.warn("生成回答异常", e);
                    return null;
                }
            });

            // 等待最多10秒
            String answer = future.get(60, TimeUnit.SECONDS);
            if (answer != null && !answer.trim().isEmpty()) {
                return answer;
            }
        } catch (TimeoutException e) {
            log.warn("生成回答超时，使用降级方案");
        } catch (Exception e) {
            log.warn("生成回答失败", e);
        }

        // 降级方案：返回简单格式
        return generateSimpleAnswer(question, sql, queryResult);
    }

    /**
     * 生成简单格式的回答
     */
    private String generateSimpleAnswer(String question, String sql, QueryResult queryResult) {
        StringBuilder sb = new StringBuilder();

        if (question.contains("总数") || question.contains("count") || sql.toUpperCase().contains("COUNT(")) {
            sb.append("统计完成，共 ").append(queryResult.getRowCount()).append(" 条记录。\n");

            // 显示分组结果
            if (queryResult.getData() != null && !queryResult.getData().isEmpty()) {
                sb.append("\n分组统计结果：\n");
                for (Map<String, Object> row : queryResult.getData()) {
                    sb.append("- ").append(row).append("\n");
                }
            }
        } else {
            sb.append("查询到 ").append(queryResult.getRowCount()).append(" 条记录。");

            if (queryResult.getRowCount() > 0 && queryResult.getRowCount() <= 10) {
                sb.append("\n详细数据：\n");
                for (int i = 0; i < queryResult.getData().size(); i++) {
                    sb.append(i + 1).append(". ").append(queryResult.getData().get(i)).append("\n");
                }
            }
        }

        return sb.toString();
    }

    /**
     * 构建成功响应
     */
    private Map<String, Object> buildSuccessResponse(String answer, String sql,
                                                     QueryResult queryResult,
                                                     DataSourceInfo dataSourceInfo) {
        Map<String, Object> result = new HashMap<>();
        result.put("response", answer);
        result.put("queryType", "data_query");
        result.put("sql", sql);
        result.put("rowCount", queryResult.getRowCount());
        result.put("executionTime", queryResult.getExecutionTime());
        result.put("dataSource", dataSourceInfo.toSimpleString());
        result.put("success", true);

        if (queryResult.getRowCount() > 0) {
            result.put("data", queryResult.getData());
        }

        return result;
    }

    /**
     * 构建错误响应
     */
    private Map<String, Object> buildErrorResponse(String message) {
        return Map.of(
                "response", message,
                "queryType", "data_query",
                "success", false
        );
    }

    private Map<String, Object> buildErrorResponse(String message, String sql) {
        return Map.of(
                "response", message,
                "queryType", "data_query",
                "sql", sql,
                "success", false
        );
    }

    /**
     * 数据源信息内部类
     */
    private static class DataSourceInfo {
        private String type;
        private String host;
        private Integer port;
        private String database;
        private String username;
        private String password;
        private String schema;
        private List<String> allowedTables; // 这个字段现在不会用于表名查询

        public boolean isIncomplete() {
            return type == null || host == null || port == null ||
                    database == null || username == null || password == null;
        }

        public String getMissingFields() {
            List<String> missing = new ArrayList<>();
            if (type == null) missing.add("数据库类型");
            if (host == null) missing.add("主机地址");
            if (port == null) missing.add("端口");
            if (database == null) missing.add("数据库名");
            if (username == null) missing.add("用户名");
            if (password == null) missing.add("密码");
            return String.join(", ", missing);
        }

        public String toSimpleString() {
            if (type != null && host != null && port != null && database != null) {
                return String.format("%s://%s:%d/%s", type, host, port, database);
            }
            return "数据源信息不完整";
        }

        @Override
        public String toString() {
            return String.format(
                    "DataSourceInfo{type='%s', host='%s', port=%d, database='%s', username='%s', schema='%s', allowedTables=%s}",
                    type, host, port, database, username, schema, allowedTables
            );
        }

        // Getters and Setters
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public Integer getPort() { return port; }
        public void setPort(Integer port) { this.port = port; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }
        public List<String> getAllowedTables() { return allowedTables; }
        public void setAllowedTables(List<String> allowedTables) { this.allowedTables = allowedTables; }
    }
}