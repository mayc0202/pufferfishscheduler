package com.pufferfishscheduler.worker.task.metadata.editor;

import com.pufferfishscheduler.common.constants.Constants;
import org.springframework.stereotype.Component;

/**
 * MySQL查询编辑器
 */
@Component
public class MySQLQueryEditor extends AbstractQueryEditor {
    /**
     * 拼写增量SQL
     * 
     * @param sql            原始查询SQL
     * @param incrementType  增量类型
     * @param incrementField 增量字段
     * @return 拼写后的增量SQL
     */
    @Override
    public String spellIncrementSql(String sql, String incrementType, String incrementField) {
        if (Constants.INCREMENT_TYPE.NUMBER_TYPE.equals(incrementType)) {
            return String.format("select * from (%s) as tmp where tmp.%s > ${maxValue}", sql, incrementField);
        } else {
            return String.format(
                    "select * from (%s) as tmp where tmp.%s > '${maxValue}' and tmp.%s < '${sourceMaxValue}'", sql,
                    incrementField, incrementField);
        }
    }

    /**
     * 拼写最大时间戳SQL
     * 
     * @param sql             原始查询SQL
     * @param incrementType   增量类型
     * @param incrementField  增量字段
     * @param timestampOffset 时间戳偏移量
     * @return 拼写后的最大时间戳SQL
     */
    @Override
    public String spellMaxValueSql(String sql, String incrementType, String incrementField, Integer timestampOffset) {
        if (Constants.INCREMENT_TYPE.NUMBER_TYPE.equals(incrementType)) {
            return String.format("SELECT MAX(tmp.%s) AS source_max_value FROM (%s) tmp", incrementField, sql);
        } else {
            return String.format("SELECT DATE_SUB(\n" +
                    "    (SELECT MAX(tmp.%s) FROM (%s) tmp),\n" +
                    "    INTERVAL %s SECOND\n" +
                    ") AS source_max_value", incrementField, sql, timestampOffset);
        }
    }

}
