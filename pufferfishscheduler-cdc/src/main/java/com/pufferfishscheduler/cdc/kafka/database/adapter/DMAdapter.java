package com.pufferfishscheduler.cdc.kafka.database.adapter;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.JdbcUrlUtil;
import com.pufferfishscheduler.common.utils.JdbcUtil;
import com.pufferfishscheduler.common.utils.MD5Util;
import com.pufferfishscheduler.cdc.kafka.database.DataSourceAdapter;
import com.pufferfishscheduler.cdc.kafka.entity.DataSyncTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 达梦数据库适配器
 *
 * @author Mayc
 * @since 2026-03-16  13:28
 */
@Slf4j
public class DMAdapter extends DataSourceAdapter {

    @Override
    public void truncateTables(List<String> tables) {
        if (tables == null || tables.isEmpty() || StringUtils.isEmpty(connectionInfo.getDbSchema())) {
            return;
        }

        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(JdbcUtil.getDriver(connectionInfo.getType()), getJdbcUrlString(false), connectionInfo);
            for (String table : tables) {
                PreparedStatement stats = null;
                try {
                    String sql = String.format("truncate table \"%s\".%s", connectionInfo.getDbSchema(), table);
                    log.info(String.format("清理目标【%s】表数据！SQL: %s", table, sql));
                    stats = conn.prepareStatement(sql);
                    stats.executeUpdate();
                } catch (Exception e) {
                    log.error("", e);
                    throw new BusinessException(e.getMessage());
                } finally {
                    JdbcUtil.close(stats);
                }
            }
        } catch (Exception e) {
            log.error("", e);
            throw new BusinessException(e.getMessage());
        } finally {
            JdbcUtil.close(conn);
        }
    }

    /**
     * 构建达梦数据库源连接器配置
     *
     * @param task 数据同步任务
     * @return
     */
    @Override
    public NewConnectorDefinition buildSourceConnector(DataSyncTask task) {
        throw new BusinessException("暂不支持DM数据源建立源连接器！");
    }

    /**
     * 获取达梦数据库JDBC连接字符串
     * @param useProperties 是否使用连接属性
     * @return JDBC连接字符串
     */
    @Override
    public String getJdbcUrlString(boolean useProperties) {
        String jdbcUrl = JdbcUrlUtil.getUrl(connectionInfo.getType(), connectionInfo.getDbHost(), connectionInfo.getDbPort(), connectionInfo.getDbSchema(), connectionInfo.getExtConfig());
        if (useProperties) {
            jdbcUrl = super.addProperties(jdbcUrl);
        }
        return jdbcUrl;
    }

    /**
     * 构建Kafka Topic名称，格式：服务名.数据库名.表名
     *
     * @param taskId   数据同步任务ID
     * @param tableName 表名
     */
    @Override
    public String buildTopicName(Integer taskId, String tableName) {
        // Kafka Topic 名称只允许部分字符，非法表名用 MD5 摘要替代，避免超长 & 非法字符
        if (!isValidTopicName(tableName)) {
            String md5 = MD5Util.encode(tableName);
            tableName = md5 != null ? "T" + md5 : "T";
        }
        return String.format("%s.%s.%s", getServerName(taskId), connectionInfo.getDbSchema(), tableName);
    }

    /**
     * 构建目标表名，格式：数据库名.表名
     *
     * @param task      数据同步任务
     * @param tableName 表名
     * @return 目标表名
     */
    @Override
    public String buildFormatTargetTableName(DataSyncTask task, String tableName) {
        return getTargetTableFullPathString(task.getTargetDatabase().getConnectionInfo().getDbSchema(), tableName);
    }
}
