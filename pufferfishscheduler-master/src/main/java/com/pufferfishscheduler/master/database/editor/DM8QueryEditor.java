package com.pufferfishscheduler.master.database.editor;

import org.springframework.stereotype.Component;

import com.pufferfishscheduler.common.constants.Constants;

/**
 * DM8查询编辑器
 */
@Component
public class DM8QueryEditor extends AbstractQueryEditor {

    /**
     * 拼写增量查询SQL
     * 
     * @param sql            原始SQL
     * @param incrementType  增量类型
     * @param incrementField 增量字段
     * @return 增量查询SQL
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
     * 拼写增量查询SQL
     * 
     * @param sql             原始SQL
     * @param incrementType   增量类型
     * @param incrementField  增量字段
     * @param timestampOffset 时间戳偏移量
     * @return 增量查询SQL
     */
    @Override
    public String spellMaxValueSql(String sql, String incrementType, String incrementField, Integer timestampOffset) {
        if (Constants.INCREMENT_TYPE.NUMBER_TYPE.equals(incrementType)) {
            return String.format("SELECT MAX(tmp.%s) AS source_max_value FROM (%s) tmp", incrementField, sql);
        } else {
            return String.format("SELECT DATEADD( SECOND, -%s,\n" +
                    "    (SELECT MAX(tmp.%s) FROM (%s) tmp)\n" +
                    ") AS source_max_value", timestampOffset, incrementField, sql);
        }
    }

}
