package com.pufferfishscheduler.plugin.util;

import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleStepException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;

/**
 * JDBC 数据库连接工具类
 * 优化点：资源安全释放、异常规范、代码整洁、空值安全、线程安全、可维护性
 */
public class JdbcUtil {

    /**
     * 工具类禁止实例化
     */
    private JdbcUtil() {
        throw new AssertionError("工具类不可实例化");
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String driverName, String url, DbDatabaseVo dbVo) throws KettleStepException {
        if (StringUtils.isBlank(driverName) || StringUtils.isBlank(url) || dbVo == null) {
            throw new KettleStepException("驱动类、连接地址、数据库信息不能为空");
        }

        Connection conn = null;
        try {
            // 加载驱动
            Class.forName(driverName);
            DriverManager.setLoginTimeout(10);

            Properties properties = new Properties();
            // 基础认证信息
            properties.setProperty("user", dbVo.getUsername());
            properties.setProperty("password", dbVo.getPassword());

            // Schema 适配
            if (StringUtils.isNotBlank(dbVo.getDbSchema())) {
                if (Constants.DbType.postgresql.equals(dbVo.getType())) {
                    properties.setProperty("currentSchema", dbVo.getDbSchema());
                } else {
                    properties.setProperty("schema", dbVo.getDbSchema());
                }
            }

            // MySQL 系列默认优化参数
            if (Constants.DbType.mysql.equals(dbVo.getType())) {
                properties.setProperty("useCursorFetch", "true");
                properties.setProperty("useSSL", "false");
                properties.setProperty("useUnicode", "true");
                properties.setProperty("characterEncoding", "UTF-8");
                properties.setProperty("zeroDateTimeBehavior", "convertToNull");
                properties.setProperty("allowMultiQueries", "true");
                properties.setProperty("serverTimezone", "Asia/Shanghai");
                properties.setProperty("useOldAliasMetadataBehavior", "true");
            }

            // 自定义属性覆盖
            if (StringUtils.isNotBlank(dbVo.getProperties())) {
                HashMap<String, String> customProps = JSONObject.parseObject(dbVo.getProperties(), HashMap.class);
                if (customProps != null && !customProps.isEmpty()) {
                    properties.putAll(customProps);
                }
            }

            conn = DriverManager.getConnection(url, properties);
            return conn;
        } catch (ClassNotFoundException e) {
            throw new KettleStepException("加载数据库驱动失败：" + e.getMessage(), e);
        } catch (SQLException e) {
            throw new KettleStepException("建立数据库连接失败：" + e.getMessage(), e);
        } catch (Exception e) {
            throw new KettleStepException("数据库连接异常：" + e.getMessage(), e);
        }
    }

    /**
     * 获取 PreparedStatement
     */
    public static PreparedStatement prepareStatement(Connection connection, String sql) throws KettleStepException {
        if (connection == null || StringUtils.isBlank(sql)) {
            throw new KettleStepException("连接对象或SQL语句不能为空");
        }
        try {
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new KettleStepException("创建PreparedStatement失败：" + e.getMessage(), e);
        }
    }

    // ========================= 统一关闭方法（最推荐） =========================
    public static void close(ResultSet rs, Statement stmt, Connection conn) throws KettleStepException {
        close(rs);
        close(stmt);
        close(conn);
    }

    // ========================= 单独关闭 =========================
    public static void close(Connection conn) throws KettleStepException {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new KettleStepException("关闭Connection失败：" + e.getMessage(), e);
            }
        }
    }

    public static void close(Statement stmt) throws KettleStepException {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new KettleStepException("关闭Statement失败：" + e.getMessage(), e);
            }
        }
    }

    public static void close(ResultSet rs) throws KettleStepException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new KettleStepException("关闭ResultSet失败：" + e.getMessage(), e);
            }
        }
    }

    // ========================= 事务 =========================
    public static void commit(Connection conn) throws KettleStepException {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new KettleStepException("事务提交失败：" + e.getMessage(), e);
            }
        }
    }

    public static void rollback(Connection conn) throws KettleStepException {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new KettleStepException("事务回滚失败：" + e.getMessage(), e);
            }
        }
    }

    // ========================= 兼容旧方法（不推荐使用） =========================
    @Deprecated
    public static void closeConnection(Connection conn) throws KettleStepException {
        close(conn);
    }

    @Deprecated
    public static void closeStatement(Statement state) throws KettleStepException {
        close(state);
    }

    @Deprecated
    public static void closeResultSet(ResultSet rs) throws KettleStepException {
        close(rs);
    }

    @Deprecated
    public static PreparedStatement preparedStatement(Connection connection, String sql) throws KettleStepException {
        return prepareStatement(connection, sql);
    }
}