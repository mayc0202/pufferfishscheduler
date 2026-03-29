package com.pufferfishscheduler.common.condition;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.model.database.DatabaseField;
import com.pufferfishscheduler.domain.vo.database.DatabaseTableFieldVo;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;


/**
 * SQL 构建 + 条件拼接 + 语法校验 + 结果映射 综合工具类
 * 整合原 SqlBuildUtils + DealConditionUtil 能力
 *
 * @author mayc
 */
public final class SqlConditionBuilder {

    private static final SimpleDateFormat DATE_FORMATTER =
            new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

    private SqlConditionBuilder() {
    }

    // -------------------------------------------------------------------------
    // 统一入口：根据数据库类型生成完整查询SQL（来自原 SqlBuildUtils）
    // -------------------------------------------------------------------------
    public static String generateQuerySql(String dbType, Integer pageNo, Integer pageSize,
                                          List<DatabaseField> fieldVoList, String dbSchema,
                                          String tableName, String searchKeyword) {
        switch (dbType) {
            case Constants.DATABASE_TYPE.MYSQL:
            case Constants.DATABASE_TYPE.DORIS:
            case Constants.DATABASE_TYPE.STAR_ROCKS:
                return buildMySqlStyleSql(fieldVoList, tableName, searchKeyword, pageNo, pageSize);
            case Constants.DATABASE_TYPE.ORACLE:
                return buildOracleSql(fieldVoList, dbSchema, tableName, searchKeyword, pageNo, pageSize);
            case Constants.DATABASE_TYPE.POSTGRESQL:
            case Constants.DATABASE_TYPE.DM8:
                return buildPostgreSql(fieldVoList, dbSchema, tableName, searchKeyword, pageNo, pageSize);
            case Constants.DATABASE_TYPE.SQL_SERVER:
                return buildSqlServerSql(fieldVoList, dbSchema, tableName, searchKeyword, pageNo, pageSize);
            default:
                throw new BusinessException("暂不支持此数据源类型！");
        }
    }

    // -------------------------------------------------------------------------
    // 各数据库 SQL 构建
    // -------------------------------------------------------------------------
    private static String buildMySqlStyleSql(List<DatabaseField> fieldVoList, String tableName,
                                             String searchKeyword, Integer pageNo, Integer pageSize) {
        if (fieldVoList.isEmpty()) return StringUtils.EMPTY;

        StringBuilder sql = new StringBuilder();
        appendSelectFields(sql, fieldVoList, "`");
        appendFromTable(sql, tableName, "`", null);
        appendSearchCondition(sql, fieldVoList, searchKeyword, "`");
        appendMySqlPagination(sql, pageNo, pageSize);
        return sql.toString();
    }

    private static String buildOracleSql(List<DatabaseField> fieldVoList, String dbSchema, String tableName,
                                         String searchKeyword, Integer pageNo, Integer pageSize) {
        if (fieldVoList.isEmpty()) return StringUtils.EMPTY;

        StringBuilder sql = new StringBuilder();
        appendSelectFields(sql, fieldVoList, "\"");
        appendFromTable(sql, tableName, "\"", dbSchema);
        appendSearchCondition(sql, fieldVoList, searchKeyword, "\"");

        return (pageNo != null && pageSize != null)
                ? wrapOraclePagination(sql.toString(), pageNo, pageSize)
                : sql.toString();
    }

    private static String buildPostgreSql(List<DatabaseField> fieldVoList, String dbSchema, String tableName,
                                          String searchKeyword, Integer pageNo, Integer pageSize) {
        if (fieldVoList.isEmpty()) return StringUtils.EMPTY;

        StringBuilder sql = new StringBuilder();
        appendSelectFields(sql, fieldVoList, "\"");
        appendFromTable(sql, tableName, "\"", dbSchema);
        appendSearchCondition(sql, fieldVoList, searchKeyword, "\"");
        appendPostgrePagination(sql, pageNo, pageSize);
        return sql.toString();
    }

    private static String buildSqlServerSql(List<DatabaseField> fieldVoList, String dbSchema, String tableName,
                                            String searchKeyword, Integer pageNo, Integer pageSize) {
        if (fieldVoList.isEmpty()) return StringUtils.EMPTY;

        StringBuilder sql = new StringBuilder();
        appendSelectFields(sql, fieldVoList, "\"");
        appendFromTable(sql, tableName, "\"", dbSchema);
        appendSqlServerSearchCondition(sql, fieldVoList, searchKeyword, "\"");

        return (pageNo != null && pageSize != null)
                ? wrapSqlServerPagination(sql.toString(), pageNo, pageSize)
                : sql.toString();
    }

    // -------------------------------------------------------------------------
    // 公共抽取：字段、表名、WHERE 条件
    // -------------------------------------------------------------------------
    private static void appendSelectFields(StringBuilder sql, List<DatabaseField> fieldVoList, String quote) {
        sql.append("SELECT ");
        for (int i = 0; i < fieldVoList.size(); i++) {
            DatabaseField field = fieldVoList.get(i);
            sql.append(quote).append(field.getName()).append(quote);

            if (StringUtils.isNotBlank(field.getBusinessName())
                    && !field.getBusinessName().equals(field.getName())) {
                sql.append(" AS ").append(field.getBusinessName());
            }
            if (i < fieldVoList.size() - 1) sql.append(", ");
        }
    }

    private static void appendFromTable(StringBuilder sql, String tableName, String quote, String dbSchema) {
        sql.append(" FROM ");
        if (StringUtils.isNotBlank(dbSchema)) {
            sql.append(quote).append(dbSchema).append(quote)
                    .append(".")
                    .append(quote).append(tableName).append(quote);
        } else {
            sql.append(quote).append(tableName).append(quote);
        }
    }

    private static void appendSearchCondition(StringBuilder sql, List<DatabaseField> fieldVoList,
                                              String searchKeyword, String quote) {
        if (StringUtils.isBlank(searchKeyword)) return;

        sql.append(" WHERE 1=1 AND (");
        for (int i = 0; i < fieldVoList.size(); i++) {
            DatabaseField field = fieldVoList.get(i);
            sql.append(quote).append(field.getName()).append(quote)
                    .append(" LIKE CONCAT('%', '").append(searchKeyword).append("', '%')");
            if (i < fieldVoList.size() - 1) sql.append(" OR ");
        }
        sql.append(")");
    }

    private static void appendSqlServerSearchCondition(StringBuilder sql, List<DatabaseField> fieldVoList,
                                                       String searchKeyword, String quote) {
        if (StringUtils.isBlank(searchKeyword)) return;

        sql.append(" WHERE 1=1 AND (");
        for (int i = 0; i < fieldVoList.size(); i++) {
            DatabaseField field = fieldVoList.get(i);
            sql.append(quote).append(field.getName()).append(quote)
                    .append(" LIKE '%' + '").append(searchKeyword).append("' + '%'");
            if (i < fieldVoList.size() - 1) sql.append(" OR ");
        }
        sql.append(")");
    }

    // -------------------------------------------------------------------------
    // 分页
    // -------------------------------------------------------------------------
    private static void appendMySqlPagination(StringBuilder sql, Integer pageNo, Integer pageSize) {
        if (pageNo == null || pageSize == null) return;
        int offset = (pageNo - 1) * pageSize;
        sql.append(" LIMIT ").append(offset).append(", ").append(pageSize);
    }

    private static void appendPostgrePagination(StringBuilder sql, Integer pageNo, Integer pageSize) {
        if (pageNo == null || pageSize == null) return;
        int offset = (pageNo - 1) * pageSize;
        sql.append(" LIMIT ").append(pageSize).append(" OFFSET ").append(offset);
    }

    private static String wrapOraclePagination(String sql, int pageNo, int pageSize) {
        int start = (pageNo - 1) * pageSize + 1;
        int end = pageNo * pageSize;
        return "SELECT * FROM (" +
                "SELECT temp.*, ROWNUM RN_FOR_QUERY " +
                "FROM (" + sql + ") temp " +
                "WHERE ROWNUM <= " + end +
                ") WHERE RN_FOR_QUERY >= " + start;
    }

    private static String wrapSqlServerPagination(String sql, int pageNo, int pageSize) {
        int start = (pageNo - 1) * pageSize + 1;
        int end = pageNo * pageSize;

        int orderIndex = sql.toLowerCase().indexOf("order by");
        String orderPart = orderIndex == -1 ? " 0 " : sql.substring(orderIndex);

        String rowNumberSql = sql.substring(0, 6) +
                " TOP " + end +
                " ROW_NUMBER() OVER(" + orderPart + ") AS rownumber," +
                sql.substring(6);

        return "SELECT * FROM (" + rowNumberSql + ") temp WHERE rownumber >= " + start;
    }

    private static String wrapCachePagination(String sql, int pageNo, int pageSize) {
        int start = (pageNo - 1) * pageSize + 1;
        int end = pageNo * pageSize;
        return "SELECT * FROM (" +
                "SELECT temp.*, ROW_NUMBER() RN_FOR_QUERY " +
                "FROM (" + sql + ") temp" +
                ") WHERE RN_FOR_QUERY >= " + start + " AND RN_FOR_QUERY <= " + end;
    }

    // -------------------------------------------------------------------------
    // 自定义双字段 SQL 构建（来自原 DealConditionUtil）
    // -------------------------------------------------------------------------
    public static String buildSelectSql(JSONObject conditionJson, List<DatabaseTableFieldVo> fieldCache) {
        if (fieldCache == null || fieldCache.size() < 2) {
            throw new BusinessException("[值映射]字典表：字段缓存至少需要两列");
        }

        String field1 = fieldCache.get(0).getFieldName();
        String field2 = fieldCache.get(1).getFieldName();
        String table = fieldCache.get(0).getTableName();
        String dbType = fieldCache.get(0).getDbType();
        String quote = getFieldQuote(dbType);

        return new StringBuilder(64)
                .append("SELECT ")
                .append(quote).append(field1).append(quote)
                .append(",")
                .append(quote).append(field2).append(quote)
                .append(" FROM ").append(table)
                .append(buildWhereClause(conditionJson, dbType))
                .toString();
    }

    // -------------------------------------------------------------------------
    // 复杂 WHERE 条件构建（多层 AND/OR 组）
    // -------------------------------------------------------------------------
    public static String buildWhereClause(JSONObject condition, String dbType) {
        if (condition == null || condition.isEmpty()) {
            return "";
        }
        validateConditionJson(condition);

        String topLogic = condition.getString("condition"); // ALL=AND, ANY=OR
        JSONArray groups = condition.getJSONArray("conditionGroups");

        if (groups == null || groups.isEmpty()) {
            return "";
        }

        StringBuilder where = new StringBuilder(" WHERE ");
        String topJoin = Constants.CONDITION.ALL.equals(topLogic) ? Constants.CONDITION.AND : Constants.CONDITION.OR;

        for (int i = 0; i < groups.size(); i++) {
            JSONObject group = groups.getJSONObject(i);
            String part = buildGroupOrCondition(group, dbType);
            where.append(part);

            if (i < groups.size() - 1) {
                where.append(topJoin);
            }
        }
        return where.toString();
    }

    private static String buildGroupOrCondition(JSONObject group, String dbType) {
        String type = group.getString("type");

        // 单个条件
        if ("condition".equals(type)) {
            String field = group.getString("field");
            String operator = group.getString("operator");
            Object value = group.get("value");
            return buildFilterExpression(dbType, field, operator, value);
        }

        // 条件组 ( ... AND ... ) / ( ... OR ... )
        if ("group".equals(type)) {
            String logic = group.getString("logic");
            JSONArray conditions = group.getJSONArray("conditions");
            String join = Constants.CONDITION.ALL.equals(logic) ? Constants.CONDITION.AND : Constants.CONDITION.OR;

            StringBuilder sb = new StringBuilder("(");
            for (int j = 0; j < conditions.size(); j++) {
                JSONObject cond = conditions.getJSONObject(j);
                String field = cond.getString("field");
                String op = cond.getString("operator");
                Object val = cond.get("value");

                sb.append(buildFilterExpression(dbType, field, op, val));
                if (j < conditions.size() - 1) {
                    sb.append(join);
                }
            }
            sb.append(")");
            return sb.toString();
        }

        return "";
    }

    /**
     * 校验条件 JSON 格式
     */
    private static void validateConditionJson(JSONObject condition) {
        if (!condition.containsKey("condition") || !condition.containsKey("conditionGroups")) {
            throw new BusinessException("[值映射]字典表校验：条件格式错误，请检查条件配置！");
        }
    }

    /**
     * 构建扁平条件
     */
    private static void appendFlatConditions(StringBuilder where, JSONArray conditions, String joinOp, String dbType) {
        if (conditions == null || conditions.isEmpty()) return;

        for (int i = 0; i < conditions.size(); i++) {
            ConditionParam param = parseConditionParam(conditions.getJSONObject(i));
            Object value = convertValueByDataType(param.getDataType(), param.getFilterValue());
            where.append(buildFilterExpression(dbType, param.getColumnName(), param.getFilterCondition(), value));
            if (i < conditions.size() - 1) where.append(" ").append(joinOp).append(" ");
        }
    }

    /**
     * 构建分组条件
     */
    private static void appendGroupedConditions(StringBuilder where, JSONArray conditions, String joinOp, String dbType) {
        if (conditions == null || conditions.isEmpty()) return;

        for (int j = 0; j < conditions.size(); j++) {
            ConditionParam param = parseConditionParam(conditions.getJSONObject(j));
            Object value = convertValueByDataType(param.getDataType(), param.getFilterValue());
            where.append("(").append(buildFilterExpression(dbType, param.getColumnName(), param.getFilterCondition(), value)).append(") ");
            if (j < conditions.size() - 1) where.append(joinOp).append(" ");
        }
    }

    /**
     * 解析条件参数
     */
    private static ConditionParam parseConditionParam(JSONObject obj) {
        ConditionParam param = new ConditionParam();
        param.setColumnName(obj.getString("columnName"));
        param.setFilterCondition(obj.getString("filterCondition"));
        param.setDataType(obj.getString("dataType"));
        param.setFilterValue(obj.get("filterValue"));
        return param;
    }

    // -------------------------------------------------------------------------
    // 数据类型转换 & 过滤表达式构建
    // -------------------------------------------------------------------------
    public static Object convertValueByDataType(String dataType, Object filterValue) {
        if (StringUtils.isBlank(dataType)) {
            throw new BusinessException("[值映射]数据库字典表：数据类型不能为空");
        }

        String strVal = filterValue == null ? null : String.valueOf(filterValue);
        String dt = dataType.trim().toLowerCase();

        try {
            switch (dt) {
                case "varchar":
                case "text":
                case "tinytext":
                case "mediumtext":
                case "longtext":
                case "json":
                    return "'" + escapeSqlQuote(strVal) + "'";
                case "int":
                case "tinyint":
                case "smallint":
                case "mediumint":
                case "bigint":
                case "integer":
                    return Integer.valueOf(strVal);
                case "decimal":
                case "double":
                    return Double.valueOf(strVal);
                case "float":
                    return Float.valueOf(strVal);
                case "timestamp":
                    return Long.valueOf(strVal);
                case "year":
                case "datetime":
                case "date":
                    return DATE_FORMATTER.parse(strVal);
                case "blob":
                case "tinyblob":
                case "mediumblob":
                case "longblob":
                case "binary":
                case "varbinary":
                    return Integer.parseInt(strVal);
                case "char":
                    if (StringUtils.isBlank(strVal)) {
                        throw new BusinessException("[值映射]数据库字典表：char 类型过滤值为空");
                    }
                    return strVal.charAt(0);
                case "bit":
                case "byte":
                    return Byte.valueOf(strVal);
                default:
                    throw new BusinessException("[值映射]数据库字典表：不支持的数据类型：" + dataType);
            }
        } catch (NumberFormatException e) {
            throw new BusinessException("[值映射]数值格式错误：" + e.getMessage());
        } catch (ParseException e) {
            throw new BusinessException("[值映射]日期解析失败：" + e.getMessage());
        }
    }

    public static String buildFilterExpression(String dbType, String column, String op, Object value) {
        StringBuilder sb = new StringBuilder();
        appendQuotedColumn(sb, dbType, column);

        switch (op) {
            case Constants.OperatorType.equal:       sb.append(" = ").append(value); break;
            case Constants.OperatorType.great:       sb.append(" > ").append(value); break;
            case Constants.OperatorType.less:        sb.append(" < ").append(value); break;
            case Constants.OperatorType.greatEqual:  sb.append(" >= ").append(value); break;
            case Constants.OperatorType.lessEqual:   sb.append(" <= ").append(value); break;
            case Constants.OperatorType.notEqual:    sb.append(" <> ").append(value); break;
            case Constants.OperatorType.startWith:      sb.append(" LIKE '").append(escapeLikeValue(value)).append("%'"); break;
            case Constants.OperatorType.endWith:       sb.append(" LIKE '%").append(escapeLikeValue(value)).append("'"); break;
            case Constants.OperatorType.like:        sb.append(" LIKE '%").append(escapeLikeValue(value)).append("%'"); break;
            default: throw new BusinessException("不支持的操作符：" + op);
        }
        return sb.toString();
    }

    private static void appendQuotedColumn(StringBuilder sb, String dbType, String column) {
        if (Constants.DATABASE_TYPE.POSTGRESQL.equals(dbType)) {
            sb.append(column);
        } else {
            sb.append("`").append(column).append("`");
        }
    }

    private static String getFieldQuote(String dbType) {
        if (dbType == null) return "";
        switch (dbType) {
            case Constants.DATABASE_TYPE.MYSQL:
            case Constants.DATABASE_TYPE.DORIS:
            case Constants.DATABASE_TYPE.DM8:
                return "`";
            default:
                return "";
        }
    }

    private static String escapeLikeValue(Object value) {
        return escapeSqlQuote(String.valueOf(value)).replace("'", "");
    }

    private static String escapeSqlQuote(String s) {
        return s == null ? "" : s.replace("'", "''");
    }

    // -------------------------------------------------------------------------
    // SQL 语法校验
    // -------------------------------------------------------------------------
    public static void validateSqlSyntax(String sql, DatabaseConnectionInfo connInfo) {
        try (Connection conn = JdbcUtil.getConnection(
                JdbcUrlUtil.getDriver(connInfo.getType()),
                JdbcUrlUtil.getUrl(connInfo.getType(), connInfo.getDbHost(), connInfo.getDbPort(), connInfo.getDbName(), connInfo.getProperties()),
                connInfo);
             Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery(sql)) {
            // 执行成功即语法合法
        } catch (SQLException e) {
            throw new BusinessException("SQL 语法校验失败：" + e.getMessage());
        }
    }

    public static void validateCustomSql(String sql) {
        if (sql.contains("*")) {
            throw new BusinessException("[值映射]自定义SQL禁止使用 *，请明确指定两列字段");
        }
        if (isInvalidColumnCount(sql)) {
            throw new BusinessException("[值映射]自定义SQL只能查询两列字段");
        }
    }

    public static boolean isInvalidColumnCount(String sql) {
        int comma = 0, semi = 0;
        for (char c : sql.toCharArray()) {
            if (c == ',') comma++;
            else if (c == ';') semi++;
        }
        return comma > 1 || semi > 1;
    }

    // -------------------------------------------------------------------------
    // 结果集映射（预览数据）
    // -------------------------------------------------------------------------
    public static void mapPreviewResult(Map<String, Object> result, ResultSet rs, ResultSetMetaData meta) throws SQLException {
        List<String> fieldList = new ArrayList<>(2);
        List<List<Object>> dataList = new ArrayList<>();

        while (rs.next()) {
            List<Object> row = new ArrayList<>(meta.getColumnCount());
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                row.add(readColumnValue(rs, meta, i));
            }
            dataList.add(row);
        }

        if (meta.getColumnCount() >= 1) fieldList.add(meta.getColumnName(1));
        if (meta.getColumnCount() >= 2) fieldList.add(meta.getColumnName(2));

        result.put("fieldList", fieldList);
        result.put("dataList", dataList);
    }

    public static Object readColumnValue(ResultSet rs, ResultSetMetaData meta, int idx) throws SQLException {
        switch (meta.getColumnType(idx)) {
            case Types.INTEGER:      return rs.getInt(idx);
            case Types.BIGINT:       return rs.getBigDecimal(idx);
            case Types.FLOAT:        return rs.getFloat(idx);
            case Types.DOUBLE:       return rs.getDouble(idx);
            case Types.VARCHAR:      return rs.getString(idx);
            case Types.BOOLEAN:      return rs.getBoolean(idx);
            case Types.DATE:         return rs.getDate(idx);
            case Types.TIME:         return rs.getTime(idx);
            case Types.TIMESTAMP:    return rs.getTimestamp(idx);
            default:                 return rs.getObject(idx);
        }
    }

    // ========================= 预览数据专用（整合新增） =========================
    /**
     * 构建带schema的全表名（统一管理，避免各业务拼接不一致）
     */
    public static String buildFullTableName(String dbType, String schema, String dbName, String tableName) {
        // 表名不能为空
        if (StringUtils.isBlank(tableName)) {
            throw new BusinessException("表名不能为空！");
        }

        // 没有 schema 直接返回表名
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }

        // 有 schema 才拼接
        return switch (dbType) {
            case Constants.DATABASE_TYPE.ORACLE, Constants.DATABASE_TYPE.POSTGRESQL, Constants.DATABASE_TYPE.DM8,
                 Constants.DATABASE_TYPE.SQL_SERVER -> String.format("\"%s\".\"%s\"", schema, tableName);
            case Constants.DATABASE_TYPE.MYSQL, Constants.DATABASE_TYPE.DORIS, Constants.DATABASE_TYPE.STAR_ROCKS ->
                    String.format("`%s`.`%s`", dbName, tableName);
            default -> dbName + "." + tableName;
        };
    }

    /**
     * 自定义SQL清理：去除分号、去除limit、保证安全
     */
    public static String cleanPreviewSql(String sql) {
        if (StringUtils.isBlank(sql)) return "";
        // 去除末尾分号
        String cleaned = sql.replace(";", "").trim();
        // 去除已有的limit（防止重复）
        int limitIdx = cleaned.toLowerCase().indexOf("limit");
        if (limitIdx > 0) {
            cleaned = cleaned.substring(0, limitIdx).trim();
        }
        return cleaned;
    }

    /**
     * 预览SQL统一加1条限制（自动适配数据库）
     */
    public static String buildPreviewLimitSql(String sql, String dbType) {
        if (StringUtils.isBlank(sql)) return sql;

        String lowerSql = sql.toLowerCase();
        if (lowerSql.contains("limit") || lowerSql.contains("top")) {
            return sql;
        }

        switch (dbType) {
            case Constants.DATABASE_TYPE.ORACLE:
                return wrapOraclePagination(sql, 1, 1);
            case Constants.DATABASE_TYPE.SQL_SERVER:
                return wrapSqlServerPagination(sql, 1, 1);
            default:
                // MySQL/DM/PostgreSQL/Kyuubi/Doris 通用
                return sql + " LIMIT 1";
        }
    }
}
