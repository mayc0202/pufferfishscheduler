package com.pufferfishscheduler.plugin.util;


public interface Constants {

    interface DataBaseType {
        String mysql = "MySQL";
        String oracle = "Oracle";
        String sqlServer = "MS SQL Server";
        String postgresql = "PostgreSQL";
        String mongoDB = "MongoDB";
        String dm = "DM8";
        String cache = "Cache";
        String doris = "Doris";
    }

    interface DbType {
        String mysql = "MySQL";
        String oracle = "Oracle";
        String sqlServer = "SQLServer";
        String postgresql = "PostgreSQL";
        String dm = "DM8";
        String starRocks = "StarRocks";
        String doris = "Doris";
    }

    interface ORACLE_CONNECT_TYPE {
        String SID = "sid";
        String SERVICE = "service";
    }

    interface DATABASE_EXT_CONFIG{
        String BE_ADDRESS = "beAddress";
        String FE_ADDRESS = "feAddress";
        String ORACLE_CONNECT_TYPE = "connectType";

    }
}

