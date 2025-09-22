package com.pufferfishscheduler.service.database.db.connector.relationdb.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.service.database.db.connector.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Doris
 */
@Slf4j
public class DorisConnector extends AbstractDatabaseConnector {

    private static String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String URL_FORMAT = "jdbc:mysql://%s:%s/%s?useCursorFetch=true&useSSL=false&useUnicode=yes&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&serverTimezone=Asia/Shanghai";

    /**
     * 表信息
     */
    private static final String TABLE_INFO_SQL = "SELECT `TABLE_NAME`,TABLE_COMMENT,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ";

    /**
     * 列信息
     */
    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE,COLUMN_KEY,ORDINAL_POSITION" +
                    " FROM INFORMATION_SCHEMA.COLUMNS  " +
                    "WHERE TABLE_SCHEMA = ? ";

    /**
     * 获取含有主键约束的表信息
     */
    private static final String PRIMARY_CONSTRAINT_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE  COLUMN_KEY = 'UNI' AND TABLE_SCHEMA = ?";

    /**
     * 获取建表语句
     */
    private static final String CREATE_TABLE_SQL = "SHOW CREATE TABLE `%s`";

    private static final String SHOW_INDEX_SQL = "SHOW INDEX FROM `%s`";


    /**
     * 构建连接器
     *
     * @return
     */
    @Override
    public AbstractDatabaseConnector build() {
        setDriver(JdbcUtil.getDriver(Constants.DATABASE_TYPE.DORIS));
        setUrl(JdbcUtil.getUrl(Constants.DATABASE_TYPE.DORIS, getHost(), String.valueOf(getPort()), getDbName(), getExtConfig()));
        return this;
    }

    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames) {
        return Collections.emptyMap();
    }

    /**
     * 获取连接
     *
     * @return
     */
    @Override
    public Connection getConnection() {
        Connection conn;
        try {
            conn = JdbcUtil.getConnection(getDriver(), getUrl(), getDatabaseInfo());
        } catch (Exception e) {
            log.error(null, e);
            throw new BusinessException("创建数据库连接失败！" + e.getMessage());
        }

        return conn;
    }

    @Override
    public List<TableForeignKey> getForeignKeys() {
        return Collections.emptyList();
    }

    @Override
    public List<TableIndexSchema> getIndexes() {
        return Collections.emptyList();
    }
}
