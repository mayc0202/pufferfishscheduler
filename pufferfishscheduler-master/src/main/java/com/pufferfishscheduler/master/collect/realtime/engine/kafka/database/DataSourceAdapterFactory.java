package com.pufferfishscheduler.master.collect.realtime.engine.kafka.database;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.database.adapter.*;

/**
 * 数据源适配器工厂：根据数据库类型创建对应的 {@link DataSourceAdapter} 实现。
 * <p>
 * 目前仅支持 MySQL，后续如需支持 Oracle/PostgreSQL 等，在此集中扩展。
 *
 * @author Mayc
 * @since 2026-03-16 10:39
 */
public class DataSourceAdapterFactory {

    /**
     * 根据数据库类型字符串获取适配器。
     * <p>
     * 兼容大小写和前后空格，例如 \"mysql\"、\" MySQL \"。
     *
     * @param type 数据库类型字符串（如 \"MySQL\"）
     * @return 对应的数据源适配器
     */
    public static DataSourceAdapter getDataSourceAdapter(String type) {
        if (type == null || type.trim().isEmpty()) {
            throw new BusinessException("数据源类型不能为空！");
        }
        String normalized = type.trim();
        try {
            DataSourceAdapter.DataBaseType dataBaseType =
                    DataSourceAdapter.DataBaseType.valueOf(normalized);
            return getDataSourceAdapter(dataBaseType);
        } catch (IllegalArgumentException ex) {
            throw new BusinessException(String.format("暂不支持该数据源【%s】", type));
        }
    }

    /**
     * 根据枚举类型获取适配器。
     *
     * @param type 数据库类型枚举
     * @return 对应的数据源适配器
     */
    public static DataSourceAdapter getDataSourceAdapter(DataSourceAdapter.DataBaseType type) {
        if (type == null) {
            throw new BusinessException("数据源类型不能为空！");
        }
        return switch (type) {
            case MySQL -> new MySQLAdapter();
            case Oracle -> new OracleAdapter();
            case PostgreSQL -> new PostgreSQLAdapter();
            case SQLServer -> new SQLServerAdapter();
            case DM8 -> new DMAdapter();
            case Doris -> new DorisAdapter();
            default -> throw new BusinessException(String.format("暂不支持该数据源【%s】", type.name()));
        };
    }
}
