package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.constants.Constants;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 空间数据转换工具类
 * 用于获取不同数据库的空间数据转换函数
 */
public class SpatialConversionUtil {
    
    /**
     * 默认坐标系，WGS84
     */
    public static final int DEFAULT_COORDINATE_SYSTEM = 4326;
    
    /**
     * 空间转换函数模板映射
     */
    private static final Map<String, Function<Integer, String>> SPATIAL_FUNCTION_MAP = new HashMap<>();
    
    static {
        // 初始化空间转换函数模板
        SPATIAL_FUNCTION_MAP.put(Constants.DbType.mysql, coord -> String.format("ST_GeomFromText(?, %s)", coord));
        SPATIAL_FUNCTION_MAP.put(Constants.DbType.oracle, coord -> "SDO_UTIL.FROM_WKTGEOMETRY(?)");
        SPATIAL_FUNCTION_MAP.put(Constants.DbType.sqlServer, coord -> String.format("geometry::STGeomFromText(?, %s)", coord));
        SPATIAL_FUNCTION_MAP.put(Constants.DbType.postgresql, coord -> String.format("ST_GeomFromText(?, %s)", coord));
        SPATIAL_FUNCTION_MAP.put(Constants.DbType.dm, coord -> String.format("dmgeo.ST_GeomFromText(?, %s)", coord));
    }
    
    /**
     * 获取空间转换函数
     * @param dbType 数据库类型
     * @param coordinateSystem 坐标系
     * @return 空间转换函数
     */
    public static String getSpatialConversionFunction(String dbType, Integer coordinateSystem) {
        // 验证输入参数
        if (dbType == null || dbType.trim().isEmpty()) {
            throw new IllegalArgumentException("数据库类型不能为空");
        }
        
        // 使用默认坐标系
        int coordSystem = coordinateSystem != null ? coordinateSystem : DEFAULT_COORDINATE_SYSTEM;
        
        // 获取转换函数
        Function<Integer, String> function = SPATIAL_FUNCTION_MAP.get(dbType);
        if (function == null) {
            throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
        }
        
        return function.apply(coordSystem);
    }
    
    /**
     * 获取空间转换函数（使用默认坐标系）
     * @param dbType 数据库类型
     * @return 空间转换函数
     */
    public static String getSpatialConversionFunction(String dbType) {
        return getSpatialConversionFunction(dbType, DEFAULT_COORDINATE_SYSTEM);
    }
    
    /**
     * 检查数据库类型是否支持空间转换
     * @param dbType 数据库类型
     * @return 是否支持
     */
    public static boolean isSpatialSupported(String dbType) {
        return dbType != null && SPATIAL_FUNCTION_MAP.containsKey(dbType);
    }
}
