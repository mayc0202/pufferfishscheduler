package com.pufferfishscheduler.master.database.connect.relationdb.impl;

import com.pufferfishscheduler.domain.domain.TableForeignKey;
import com.pufferfishscheduler.domain.domain.TableIndexSchema;
import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.master.database.connect.relationdb.AbstractDatabaseConnector;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Oracle数据库连接器
 *
 * @author Mayc
 * @since 2026-03-16  16:49
 */
@Slf4j
public class OracleConnector extends AbstractDatabaseConnector {

    private static String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String SERVICE_URL_FORMAT = "jdbc:oracle:thin:@//%s:%s/%s";
    private static final String SID_URL_FORMAT = "jdbc:oracle:thin:@%s:%s:%s";

    private static final String TABLE_INFO_SQL = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE FROM ("
            + " SELECT A.TABLE_NAME,B.COMMENTS as TABLE_COMMENT,b.table_type FROM ALL_TABLES A,ALL_TAB_COMMENTS B WHERE A.TABLE_NAME=B.TABLE_NAME "
            + " 					 and A.owner='${schemaName}' and B.owner='${schemaName}' "
            + " 					UNION ALL "
            + " SELECT A.VIEW_NAME AS TABLE_NAME,B.COMMENTS AS TABLE_COMMENT,b.table_type FROM ALL_VIEWS A,ALL_TAB_COMMENTS B WHERE A.VIEW_NAME=B.TABLE_NAME"
            + " 					and A.owner='${schemaName}' and B.owner='${schemaName}'"
            + "                 UNION ALL "
            + " SELECT A.TABLE_NAME ,B.COMMENTS AS TABLE_COMMENT,'VIEW' as table_type FROM ALL_TABLES A,ALL_MVIEW_COMMENTS B WHERE A.TABLE_NAME=B.MVIEW_NAME"
            + "                 and A.owner='${schemaName}' and B.owner='${schemaName}'"
            + " ) WHERE 1=1";


    private static final String COLUMN_INFO_SQL =
            "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,COLUMN_ID,DATA_SCALE,NULLABLE,DATA_DEFAULT,COMMENTS, DATA_PRECISION FROM ("
                    + " SELECT A.TABLE_NAME,A.COLUMN_NAME,A.DATA_TYPE," +
                    "CASE " +
                    " WHEN DATA_TYPE IN ('NUMBER', 'FLOAT') AND DATA_PRECISION IS NOT NULL THEN " +
                    "  TO_CHAR(DATA_PRECISION) " +
                    " WHEN DATA_TYPE IN ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2', 'RAW') THEN " +
                    "  TO_CHAR(CASE WHEN CHAR_LENGTH = 0 THEN DATA_LENGTH ELSE CHAR_LENGTH END) " +
                    " ELSE NULL " +
                    "END AS DATA_LENGTH," +
                    "A.COLUMN_ID,A.DATA_SCALE,A.NULLABLE,A.DATA_DEFAULT,A.DATA_PRECISION,B.COMMENTS"
                    + " FROM all_tab_columns A,all_col_comments B "
                    + " WHERE A.TABLE_NAME = B.TABLE_NAME  and A.COLUMN_NAME = B.COLUMN_NAME and a.owner='${schemaName}' and b.owner='${schemaName}') WHERE 1=1 ";


    private static final String COLUMN_PRI_KEY =
            " SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_TYPE FROM "
                    + " (SELECT AU.CONSTRAINT_TYPE, CU.COLUMN_NAME,CU.TABLE_NAME FROM all_CONS_COLUMNS CU,all_CONSTRAINTS AU WHERE CU.CONSTRAINT_NAME = AU.CONSTRAINT_NAME"
                    + " and cu.owner='${schemaName}' and au.owner='${schemaName}') WHERE 1=1 ";

    private static final String FK_CONSTRAINT_TABLE_SQL =
            "SELECT UC.CONSTRAINT_NAME, UC.TABLE_NAME, COLS.COLUMN_NAME, RUC.TABLE_NAME AS REFERENCED_TABLE_NAME, RCOLS.COLUMN_NAME AS REFERENCED_COLUMN_NAME " +
                    "FROM USER_CONS_COLUMNS COLS JOIN USER_CONSTRAINTS UC ON COLS.CONSTRAINT_NAME = UC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS RUC ON UC.R_CONSTRAINT_NAME = RUC.CONSTRAINT_NAME " +
                    "LEFT JOIN USER_CONS_COLUMNS RCOLS ON RUC.CONSTRAINT_NAME = RCOLS.CONSTRAINT_NAME " +
                    "WHERE UC.CONSTRAINT_TYPE = 'R' AND UC.OWNER = ?";

    private static final String IDX_CONSTRAINT_TABLE_SQL =
            "SELECT AIC.INDEX_NAME, AIC.TABLE_NAME, " +
                    "LISTAGG(AIC.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY AIC.COLUMN_NAME) AS COLUMN_NAME, " +
                    "AI.UNIQUENESS, UC.CONSTRAINT_TYPE " +
                    "FROM ALL_IND_COLUMNS AIC " +
                    "JOIN ALL_INDEXES AI ON AIC.INDEX_NAME = AI.INDEX_NAME " +
                    "LEFT JOIN USER_CONSTRAINTS UC ON UC.INDEX_NAME = AIC.INDEX_NAME " +
                    "WHERE AIC.TABLE_OWNER = ? " +
                    "GROUP BY AIC.INDEX_NAME, AIC.TABLE_NAME, AI.UNIQUENESS, UC.CONSTRAINT_TYPE";

    @Override
    public AbstractDatabaseConnector build() {
        return null;
    }

    @Override
    public Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames) {
        return Map.of();
    }

    @Override
    public Connection getConnection() {
        return null;
    }

    @Override
    public List<TableForeignKey> getForeignKeys() {
        return List.of();
    }

    @Override
    public List<TableIndexSchema> getIndexes() {
        return List.of();
    }
}
