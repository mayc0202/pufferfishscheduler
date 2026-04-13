package com.pufferfishscheduler.plugin;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.conditon.ConditionGroup;
import com.pufferfishscheduler.plugin.common.conditon.ConditionParam;
import com.pufferfishscheduler.plugin.common.conditon.ConditionTree;
import org.codehaus.janino.ExpressionEvaluator;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * 数据过滤步骤
 */
public class DataFilterStep extends BaseStep implements StepInterface {

    private DataFilterStepMeta meta;
    private DataFilterStepData data;
    private String javaCode;
    private String filterType;
    private String condition;
    private List<String> fieldName;

    // 日期格式常量
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String SOURCE_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public DataFilterStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                          TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }
        logBasic("DataFilterStep initialized.");
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        DataFilterStepData data = (DataFilterStepData) sdi;
        this.data = data;
        this.meta = (DataFilterStepMeta) smi;
        this.javaCode = this.meta.getJavaCode();
        this.filterType = this.meta.getFilterType();
        this.condition = this.meta.getCondition();

        Object[] row = getRow();
        if (row == null) {
            setOutputDone();
            return false;
        }

        // 首次调用时初始化输出元数据和字段名列表
        if (first) {
            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
            fieldName = Arrays.asList(data.outputRowMeta.getFieldNames());
            first = false;
        }

        boolean keep; // true 表示满足过滤条件（保留行）
        if (Constants.FILTER_TYPE.FILTER_1.equals(filterType)) {
            // Java 脚本模式
            keep = evaluateJavaExpression(getInputRowMeta(), row);
        } else {
            // 多条件模式
            if (condition == null || condition.trim().isEmpty()) {
                throw new KettleException("过滤条件不能为空，请检查配置。");
            }
            keep = evaluateCondition(condition, row);
        }

        // 过滤逻辑：满足条件则输出行
        if (keep) {
            // 调整行大小以匹配输出字段数
            Object[] outputRow = RowDataUtil.resizeArray(row, data.outputRowMeta.size());
            putRow(data.outputRowMeta, outputRow);
        }

        return true;
    }

    // ===================== 条件求值核心方法（基于 ConditionTree） =====================

    /**
     * 解析 JSON 多条件表达式，并递归求值
     */
    private boolean evaluateCondition(String conditionJson, Object[] row) throws KettleStepException {
        ConditionTree conditionTree = JSONObject.parseObject(conditionJson, ConditionTree.class);
        if (conditionTree == null) {
            throw new KettleStepException("无效的多条件配置 JSON。");
        }
        return evaluateConditionTree(conditionTree, row);
    }

    /**
     * 递归求值条件树（顶层）
     */
    private boolean evaluateConditionTree(ConditionTree tree, Object[] row) throws KettleStepException {
        List<ConditionGroup> groups = tree.getConditionGroups();
        if (groups == null || groups.isEmpty()) {
            return true; // 无条件，保留所有行
        }
        String logic = tree.getCondition(); // "ALL" 或 "ANY"
        boolean result = Constants.Condition.ALL.equalsIgnoreCase(logic);
        for (ConditionGroup group : groups) {
            boolean groupResult = evaluateConditionGroup(group, row);
            if (Constants.Condition.ALL.equalsIgnoreCase(logic)) {
                result = result && groupResult;
                if (!result) break;
            } else { // ANY
                result = result || groupResult;
                if (result) break;
            }
        }
        return result;
    }

    /**
     * 求值单个条件组
     */
    private boolean evaluateConditionGroup(ConditionGroup group, Object[] row) throws KettleStepException {
        List<ConditionParam> params = group.getQueryConditions();
        if (params == null || params.isEmpty()) {
            return true;
        }
        String logic = group.getCondition(); // "ALL" 或 "ANY"
        boolean result = Constants.Condition.ALL.equalsIgnoreCase(logic);
        for (ConditionParam param : params) {
            boolean paramResult = evaluateConditionParam(param, row);
            if (Constants.Condition.ALL.equalsIgnoreCase(logic)) {
                result = result && paramResult;
                if (!result) break;
            } else {
                result = result || paramResult;
                if (result) break;
            }
        }
        return result;
    }

    /**
     * 求值单个条件
     */
    private boolean evaluateConditionParam(ConditionParam param, Object[] row) throws KettleStepException {
        String columnName = param.getColumnName();
        Object filterValue = param.getFilterValue();
        String dataType = param.getDataType();
        String filterCondition = param.getFilterCondition();

        int index = fieldName.indexOf(columnName);
        if (index < 0) {
            logError("字段 " + columnName + " 不在当前行元数据中。");
            return false;
        }
        Object fieldValue = row[index];
        return evaluateFieldCondition(fieldValue, filterValue, filterCondition, dataType);
    }

    // ===================== Java 表达式求值（Janino） =====================

    /**
     * 使用 Janino 编译并执行 Java 布尔表达式
     */
    private boolean evaluateJavaExpression(RowMetaInterface rowMeta, Object[] row) throws KettleValueException, KettleException {
        try {
            if (data.expressionEvaluator == null) {
                if (javaCode == null || javaCode.trim().isEmpty()) {
                    throw new KettleValueException("Java 脚本表达式为空，请检查配置。");
                }
                String realCondition = environmentSubstitute(javaCode);
                compileExpression(realCondition);
            }

            // 准备参数
            for (int i = 0; i < data.argumentIndexes.size(); i++) {
                int idx = data.argumentIndexes.get(i);
                ValueMetaInterface meta = data.outputRowMeta.getValueMeta(idx);
                data.argumentData[i] = meta.convertToNormalStorageType(row[idx]);
            }

            Object result = data.expressionEvaluator.evaluate(data.argumentData);
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else {
                throw new KettleException("过滤表达式必须返回布尔类型，实际返回: " + result.getClass().getName());
            }
        } catch (Exception e) {
            throw new KettleValueException("Java 脚本执行失败", e);
        }
    }

    /**
     * 编译 Java 表达式，缓存参数信息
     */
    private void compileExpression(String expression) throws Exception {
        data.argumentIndexes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();
        List<Class<?>> paramTypes = new ArrayList<>();

        RowMetaInterface outputRowMeta = data.outputRowMeta;
        for (int i = 0; i < outputRowMeta.size(); i++) {
            ValueMetaInterface vmi = outputRowMeta.getValueMeta(i);
            String fieldName = vmi.getName();
            if (expression.contains(fieldName)) {
                data.argumentIndexes.add(i);
                paramNames.add(fieldName);
                paramTypes.add(mapJaninoType(vmi.getType()));
            }
        }

        data.expressionEvaluator = new ExpressionEvaluator();
        data.expressionEvaluator.setParameters(
                paramNames.toArray(new String[0]),
                paramTypes.toArray(new Class[0])
        );
        data.expressionEvaluator.setReturnType(Object.class);
        data.expressionEvaluator.setThrownExceptions(new Class[]{Exception.class});
        data.expressionEvaluator.cook(expression);
        data.argumentData = new Object[data.argumentIndexes.size()];
    }

    // ===================== 数据类型转换和比较辅助方法 =====================

    /**
     * 将 Kettle 值类型映射为 Janino 表达式可用的 Java 类型
     */
    private Class<?> mapJaninoType(int kettleType) {
        switch (kettleType) {
            case ValueMetaInterface.TYPE_NUMBER:
                return Double.class;
            case ValueMetaInterface.TYPE_STRING:
                return String.class;
            case ValueMetaInterface.TYPE_DATE:
                return Date.class;
            case ValueMetaInterface.TYPE_BOOLEAN:
                return Boolean.class;
            case ValueMetaInterface.TYPE_INTEGER:
                return Long.class;
            case ValueMetaInterface.TYPE_BIGNUMBER:
                return BigDecimal.class;
            case ValueMetaInterface.TYPE_BINARY:
                return byte[].class;
            default:
                return String.class;
        }
    }

    /**
     * 映射前端传入的数据类型标识到内部编码
     */
    private Integer mapDataType(Object dataType) {
        String type = String.valueOf(dataType);
        return switch (type) {
            case Constants.DataType.TIMESTAMP_CODE -> Constants.DataType.TIMESTAMP_VALUE;
            case Constants.DataType.STRING_CODE -> Constants.DataType.STRING_VALUE;
            case Constants.DataType.NUMBER_CODE -> Constants.DataType.NUMBER_VALUE;
            case Constants.DataType.INTERNET_ADDRESS_CODE -> Constants.DataType.INTERNET_ADDRESS_VALUE;
            case Constants.DataType.INTEGER_CODE -> Constants.DataType.INTEGER_VALUE;
            case Constants.DataType.DATE_CODE -> Constants.DataType.DATE_VALUE;
            case Constants.DataType.BOOLEAN_CODE -> Constants.DataType.BOOLEAN_VALUE;
            case Constants.DataType.BINARY_CODE -> Constants.DataType.BINARY_VALUE;
            case Constants.DataType.BIGNUMBER_CODE -> Constants.DataType.BIGNUMBER_VALUE;
            case Constants.DataType.UNDEFINED_CODE -> Constants.DataType.UNDEFINED_VALUE;
            default -> null;
        };
    }

    // ---------- 比较操作实现 ----------

    private boolean compareEqual(Object field, Object target, Integer dataTypeCode) {
        if (dataTypeCode == null) return false;
        return switch (dataTypeCode) {
            case Constants.DataType.TIMESTAMP_VALUE -> toTimestamp(field) == toTimestamp(target);
            case Constants.DataType.STRING_VALUE -> String.valueOf(field).equals(String.valueOf(target));
            case Constants.DataType.NUMBER_VALUE -> Double.compare(toDouble(field), toDouble(target)) == 0;
            case Constants.DataType.INTEGER_VALUE -> Long.compare(toLong(field), toLong(target)) == 0;
            case Constants.DataType.DATE_VALUE -> toDateMillis(field) == toDateMillis(target);
            case Constants.DataType.BOOLEAN_VALUE ->
                    Boolean.parseBoolean(String.valueOf(field)) == Boolean.parseBoolean(String.valueOf(target));
            case Constants.DataType.BINARY_VALUE ->
                    Integer.parseInt(field.toString(), 2) == Integer.parseInt(target.toString(), 2);
            case Constants.DataType.BIGNUMBER_VALUE -> toBigDecimal(field).compareTo(toBigDecimal(target)) == 0;
            default -> false;
        };
    }

    private boolean compareGreater(Object field, Object target, Integer dataTypeCode) {
        return switch (dataTypeCode) {
            case Constants.DataType.TIMESTAMP_VALUE -> toTimestamp(field) > toTimestamp(target);
            case Constants.DataType.STRING_VALUE -> String.valueOf(field).compareTo(String.valueOf(target)) > 0;
            case Constants.DataType.NUMBER_VALUE -> Double.compare(toDouble(field), toDouble(target)) > 0;
            case Constants.DataType.INTEGER_VALUE -> Long.compare(toLong(field), toLong(target)) > 0;
            case Constants.DataType.DATE_VALUE -> toDateMillis(field) > toDateMillis(target);
            case Constants.DataType.BINARY_VALUE ->
                    Integer.parseInt(field.toString(), 2) > Integer.parseInt(target.toString(), 2);
            case Constants.DataType.BIGNUMBER_VALUE -> toBigDecimal(field).compareTo(toBigDecimal(target)) > 0;
            default -> false;
        };
    }

    private boolean compareLess(Object field, Object target, Integer dataTypeCode) {
        return switch (dataTypeCode) {
            case Constants.DataType.TIMESTAMP_VALUE -> toTimestamp(field) < toTimestamp(target);
            case Constants.DataType.STRING_VALUE -> String.valueOf(field).compareTo(String.valueOf(target)) < 0;
            case Constants.DataType.NUMBER_VALUE -> Double.compare(toDouble(field), toDouble(target)) < 0;
            case Constants.DataType.INTEGER_VALUE -> Long.compare(toLong(field), toLong(target)) < 0;
            case Constants.DataType.DATE_VALUE -> toDateMillis(field) < toDateMillis(target);
            case Constants.DataType.BINARY_VALUE ->
                    Integer.parseInt(field.toString(), 2) < Integer.parseInt(target.toString(), 2);
            case Constants.DataType.BIGNUMBER_VALUE -> toBigDecimal(field).compareTo(toBigDecimal(target)) < 0;
            default -> false;
        };
    }

    private boolean compareGreaterOrEqual(Object field, Object target, Integer dataTypeCode) {
        return switch (dataTypeCode) {
            case Constants.DataType.TIMESTAMP_VALUE -> toTimestamp(field) >= toTimestamp(target);
            case Constants.DataType.STRING_VALUE -> String.valueOf(field).compareTo(String.valueOf(target)) >= 0;
            case Constants.DataType.NUMBER_VALUE -> Double.compare(toDouble(field), toDouble(target)) >= 0;
            case Constants.DataType.INTEGER_VALUE -> Long.compare(toLong(field), toLong(target)) >= 0;
            case Constants.DataType.DATE_VALUE -> toDateMillis(field) >= toDateMillis(target);
            case Constants.DataType.BINARY_VALUE ->
                    Integer.parseInt(field.toString(), 2) >= Integer.parseInt(target.toString(), 2);
            case Constants.DataType.BIGNUMBER_VALUE -> toBigDecimal(field).compareTo(toBigDecimal(target)) >= 0;
            default -> false;
        };
    }

    private boolean compareLessOrEqual(Object field, Object target, Integer dataTypeCode) {
        return switch (dataTypeCode) {
            case Constants.DataType.TIMESTAMP_VALUE -> toTimestamp(field) <= toTimestamp(target);
            case Constants.DataType.STRING_VALUE -> String.valueOf(field).compareTo(String.valueOf(target)) <= 0;
            case Constants.DataType.NUMBER_VALUE -> Double.compare(toDouble(field), toDouble(target)) <= 0;
            case Constants.DataType.INTEGER_VALUE -> Long.compare(toLong(field), toLong(target)) <= 0;
            case Constants.DataType.DATE_VALUE -> toDateMillis(field) <= toDateMillis(target);
            case Constants.DataType.BINARY_VALUE ->
                    Integer.parseInt(field.toString(), 2) <= Integer.parseInt(target.toString(), 2);
            case Constants.DataType.BIGNUMBER_VALUE -> toBigDecimal(field).compareTo(toBigDecimal(target)) <= 0;
            default -> false;
        };
    }

    // ---------- 类型转换辅助 ----------

    private double toDouble(Object obj) {
        return Double.parseDouble(obj.toString());
    }

    private long toLong(Object obj) {
        return Long.parseLong(obj.toString());
    }

    private BigDecimal toBigDecimal(Object obj) {
        return new BigDecimal(obj.toString());
    }

    /**
     * 将对象转换为时间戳（毫秒）
     */
    private long toTimestamp(Object obj) {
        if (obj instanceof Date) {
            return ((Date) obj).getTime();
        }
        String str = obj.toString().replace(".0", ""); // 处理可能的时间戳格式
        LocalDateTime ldt = parseLocalDateTime(str);
        return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 将对象解析为 LocalDateTime，支持常见格式
     */
    private LocalDateTime parseLocalDateTime(String str) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT);
        // 如果只包含日期，补上时间
        if (str.length() <= 10) {
            str += " 00:00:00";
        }
        return LocalDateTime.parse(str, formatter);
    }

    /**
     * 将对象转换为日期毫秒数（用于 Date 类型比较）
     */
    private long toDateMillis(Object obj) {
        if (obj instanceof Date) {
            return ((Date) obj).getTime();
        }
        String str = obj.toString();
        // 尝试解析原始格式
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(SOURCE_DATE_FORMAT, Locale.ENGLISH);
            Date date = sdf.parse(str);
            return date.getTime();
        } catch (ParseException e) {
            // 如果失败，使用默认格式解析
            return parseLocalDateTime(str).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }

    /**
     * 字段条件求值（根据操作符和数据类型进行比较）
     * 该方法是原有的核心比较逻辑，被 evaluateConditionParam 复用。
     */
    private boolean evaluateFieldCondition(Object fieldValue, Object targetValue, String operator, String dataType) {
        if (fieldValue == null) {
            // 对于 NULL 值的处理，可根据业务决定，这里假设 NULL 只匹配 "=" 和 "!="
            return (Constants.OperatorType.equal.equals(operator) && targetValue == null) ||
                    (Constants.OperatorType.notEqual.equals(operator) && targetValue != null);
        }

        Integer dataTypeCode = mapDataType(dataType);
        return switch (operator) {
            case Constants.OperatorType.equal -> compareEqual(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.notEqual -> !compareEqual(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.great -> compareGreater(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.less -> compareLess(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.greatEqual -> compareGreaterOrEqual(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.lessEqual -> compareLessOrEqual(fieldValue, targetValue, dataTypeCode);
            case Constants.OperatorType.startWith -> String.valueOf(fieldValue).startsWith(String.valueOf(targetValue));
            case Constants.OperatorType.endWith -> String.valueOf(fieldValue).endsWith(String.valueOf(targetValue));
            case Constants.OperatorType.contains -> String.valueOf(fieldValue).contains(String.valueOf(targetValue));
            default -> {
                logError("不支持的操作符: " + operator);
                yield false;
            }
        };
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
        logBasic("DataFilterStep disposed.");
    }
}