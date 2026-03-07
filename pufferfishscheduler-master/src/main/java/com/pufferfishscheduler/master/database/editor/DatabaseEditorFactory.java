package com.pufferfishscheduler.master.database.editor;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 数据库编辑器工厂
 */
public class DatabaseEditorFactory {

    /**
     * 数据库类型到编辑器的映射
     */
    private static final Map<String, Supplier<AbstractQueryEditor>> EDITOR_MAP = new HashMap<>();

    static {
        // 初始化编辑器映射
        EDITOR_MAP.put(Constants.DbType.mysql, MySQLQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.starRocks, MySQLQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.doris, MySQLQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.oracle, OracleQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.postgresql, PostgreSQLQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.sqlServer, SQLServerQueryEditor::new);
        EDITOR_MAP.put(Constants.DbType.dm, DM8QueryEditor::new);
    }

    /**
     * 根据数据库类型获取编辑器
     * 
     * @param dbType 数据库类型
     * @return 对应的查询编辑器
     */
    public static AbstractQueryEditor getDatabaseEditor(String dbType) {
        if (dbType == null || dbType.trim().isEmpty()) {
            throw new BusinessException("数据库类型不能为空！");
        }

        Supplier<AbstractQueryEditor> editorSupplier = EDITOR_MAP.get(dbType);
        if (editorSupplier == null) {
            throw new BusinessException("不支持的数据库类型：" + dbType);
        }

        return editorSupplier.get();
    }

    /**
     * 检查数据库类型是否支持
     * 
     * @param dbType 数据库类型
     * @return 是否支持
     */
    public static boolean isSupported(String dbType) {
        return dbType != null && EDITOR_MAP.containsKey(dbType);
    }
}
