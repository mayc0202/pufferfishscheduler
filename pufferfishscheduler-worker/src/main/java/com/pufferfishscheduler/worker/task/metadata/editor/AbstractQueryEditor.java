package com.pufferfishscheduler.worker.task.metadata.editor;

/**
 * 抽象查询编辑器
 */
public abstract class AbstractQueryEditor {

    /**
     * 拼写增量SQL
     *
     * @param sql            原始查询SQL
     * @param incrementType  增量类型
     * @param incrementField 增量字段
     * @return 拼写后的增量SQL
     */
    public abstract String spellIncrementSql(String sql, String incrementType, String incrementField);

    /**
     * 拼写最大时间戳SQL
     *
     * @param sql             原始查询SQL
     * @param incrementType   增量类型
     * @param incrementField  增量字段
     * @param timestampOffset 时间戳偏移量
     * @return 拼写后的最大时间戳SQL
     */
    public abstract String spellMaxValueSql(String sql, String incrementType, String incrementField,
            Integer timestampOffset);
}
