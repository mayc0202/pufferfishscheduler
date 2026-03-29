package com.pufferfishscheduler.domain.vo.database;

/**
 * 数据库表字段VO
 */
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 数据库表字段VO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DatabaseTableFieldVo {

    private String category;

    private String dbType;

    private String dbName;

    private String dbHost;

    private String dbPort;

    private String userName;

    private String password;

    private Integer tableId;

    private String tableName;

    private Integer fieldId;

    private String fieldName;

    private String fieldType;

    private String properties;

    private String dbSchema;
}
