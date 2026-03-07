package com.pufferfishscheduler.master.database.editor;

import org.springframework.stereotype.Component;

import com.pufferfishscheduler.common.constants.Constants;

/**
 * Oracle查询编辑器
 */
@Component
public class OracleQueryEditor extends AbstractQueryEditor {

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
            return String.format("select * from (%s) tmp where tmp.\"%s\" > ${maxValue}", sql, incrementField);
        } else {
            return String.format(
                    "select * from (%s) tmp where TO_CHAR(tmp.\"%s\", 'YYYY-MM-DD HH24:MI:SS') > '${maxValue}' and TO_CHAR(tmp.\"%s\", 'YYYY-MM-DD HH24:MI:SS') < '${sourceMaxValue}'",
                    sql, incrementField, incrementField);
        }
    }

    /**
     * 拼写最大增量值查询SQL
     * 
     * @param sql             原始SQL
     * @param incrementType   增量类型
     * @param incrementField  增量字段
     * @param timestampOffset 时间戳偏移量
     * @return 最大增量值查询SQL
     */
    @Override
    public String spellMaxValueSql(String sql, String incrementType, String incrementField, Integer timestampOffset) {
        if (Constants.INCREMENT_TYPE.NUMBER_TYPE.equals(incrementType)) {
            return String.format("SELECT MAX(tmp.\"%s\") AS source_max_value FROM (%s) tmp", incrementField, sql);
        } else {
            return String.format("SELECT \n" +
                    "    MAX(tmp.\"%s\") - INTERVAL '%s' SECOND AS source_max_value\n" +
                    "FROM \n" +
                    "    (%s) tmp", incrementField, timestampOffset, sql);
        }
    }

}
