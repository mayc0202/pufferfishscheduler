package com.pufferfishscheduler.service.database.db.connector.relationdb;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.domain.model.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * 数据源连接器抽象类
 */
@Data
@Slf4j
public abstract class AbstractDatabaseConnector {

    /**
     * 主机
     */
    private String host;

    /**
     * 端口
     */
    private Integer port;

    /**
     * 用户
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 数据源类型
     */
    private String type;

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 模式
     */
    private String schema;

    /**
     * 驱动类
     */
    private String driver;

    /**
     * url
     */
    private String url;

    /**
     * 配置
     */
    private String properties;

    /**
     * 扩展配置
     */
    private String extConfig;


    /**
     * 构建连接信息；
     *
     * @return
     */
    public abstract AbstractDatabaseConnector build();

//    /**
//     * 获取数据库下所有的表基础信息
//     * @return Map<key,Value>  key=表名称；Value=表信息
//     */
//    public abstract Map<String, DBTableInfo> getTables() throws SQLException;
//
//    /**
//     * 获取数据库中指定表名的表信息
//     * @param tableName
//     * @return
//     */
//    public abstract DBTableWithColumnInfo getTable(String tableName);
//
//    /**
//     * 获取数据库下所有的表信息，包括表的字段信息；
//     * @return Map<key,Value>  key=表名称；Value=表信息
//     */
//    public abstract Map<String, DBTableWithColumnInfo> getTableWithColumnInfos();

    /**
     * 获取数据库下所有的表信息，包括表的字段信息；
     *
     * @param inTableNames    仅包含 inTableNames 指定的表
     * @param notInTableNames 排除 notInTableNames 包含的表
     * @return Map<key,Value>  key=表名称；Value=表信息
     */
    public abstract Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames);

    /**
     * 获取数据源连接
     *
     * @return
     */
    public abstract Connection getConnection();


//    /**
//     * 获取指定表的列信息
//     * @param tableName 表名
//     * @return
//     */
//    public abstract List<DBColumnInfo> getColumns(String tableName);
//
//    /**
//     * 获取指定表的数据量
//     * @param tableName 表名
//     * @return
//     */
//    public abstract Long getDataVolumeByTableName(String tableName, Connection conn);
//
//    public abstract void checkDatabaseInfo(DatabaseForm form);
//

    /**
     * 获取所有外键
     *
     * @return
     */
    public abstract List<TableForeignKey> getForeignKeys();

    /**
     * 获取所有索引
     *
     * @return
     */
    public abstract List<TableIndexSchema> getIndexes();
//
//    public void executeIgnoreError(String sql, Connection conn) {
//        Statement stat = null;
//
//        try{
//            stat = conn.createStatement();
//
//            if(sql.endsWith(";")){
//                sql = sql.substring(0,sql.length()-1);
//            }
//            stat.execute(sql);
//
//        }catch (Exception e) {
//            //
//        }finally {
//            JdbcUtil.close(stat);
//        }
//    }

    /**
     * 测试连接
     *
     * @param dbType
     * @return
     */
    public ConResponse connect(String dbType) {
        Connection conn = null;
        Statement stat = null;
        ConResponse response = new ConResponse();
        ResultSet rs = null;
        try {

            conn = JdbcUtil.getConnection(this.driver, this.url, getDatabaseInfo());

            if (StringUtils.isNotEmpty(schema)
                    && !"".equals(schema.trim())
                    && !Constants.DATABASE_TYPE.POSTGRESQL.equals(dbType)
                    && !Constants.DATABASE_TYPE.SQL_SERVER.equals(dbType)) {
                stat = conn.createStatement();
                stat.execute(" ALTER SESSION SET CURRENT_SCHEMA = \"" + schema + "\" ");
            }
            if (StringUtils.isNotEmpty(schema) && !"".equals(schema.trim()) && (Constants.DATABASE_TYPE.POSTGRESQL.equals(dbType))) {
                stat = conn.createStatement();
                rs = stat.executeQuery("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '" + schema + "')");
                while (rs.next()) {
                    String verifyResult = rs.getString(1);
                    if ("f".equalsIgnoreCase(verifyResult)) {
                        throw new BusinessException(String.format("模式Schema【%s】不存在！", schema));
                    }
                }
            }
            if (StringUtils.isNotEmpty(schema) && !"".equals(schema.trim()) && (Constants.DATABASE_TYPE.SQL_SERVER.equals(dbType))) {
                stat = conn.createStatement();
                rs = stat.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + schema + "'");
                if (null == rs) {
                    throw new BusinessException(String.format("模式Schema【%s】不存在！", schema));
                }
            }
            response.setResult(true);
            response.setMsg("连接成功！");
        } catch (Exception e) {
            log.error(null, e);
            response.setResult(false);
            response.setMsg(String.format("连接失败! 原因:%s", e.getMessage()));
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(stat);
            JdbcUtil.closeConnection(conn);
        }

        return response;
    }

    /**
     * 封装数据源信息
     *
     * @return
     */
    public DatabaseConnectionInfo getDatabaseInfo() {
        DatabaseConnectionInfo info = new DatabaseConnectionInfo();
        info.setUsername(username);
        info.setPassword(password);
        info.setType(type);
        info.setDbSchema(schema);
        info.setProperties(properties);
        info.setExtConfig(extConfig);
        return info;
    }

}
