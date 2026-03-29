package com.pufferfishscheduler.plugin.common.custom;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Param implements Serializable {

    private static final long serialVersionUID = 1848411189744989634L;
    private Map<String,Object> rowValues=new HashMap<String, Object>();

    public Param() {

    }

    public Param(Map<String, Object> values) {
        rowValues.putAll(values);
    }

    public Param put(String key, Object value) {
        rowValues.put(key, value);
        return this;
    }

    /**
     * 存储为Long类型
     * @param key
     * @param value
     * @return
     */
    public Param setLong(String key, Object value) {
        rowValues.put(key,(Long) value);
        return this;
    }

    /**
     * 存储为字符串类型
     * @param key
     * @param value
     * @return
     */
    public Param setString(String key, Object value) {
        rowValues.put(key,String.valueOf(value));
        return this;
    }

    /**
     * 存储为Integer类型
     * @param key
     * @param value
     * @return
     */
    public Param setInteger(String key, Object value) {
        rowValues.put(key,(Integer) value);
        return this;
    }

    /**
     * 存储为Double类型
     * @param key
     * @param value
     * @return
     */
    public Param setDouble(String key, Object value) {
        rowValues.put(key,(Double) value);
        return this;
    }

    /**
     * 存储为Float类型
     * @param key
     * @param value
     * @return
     */
    public Param setFloat(String key, Object value) {
        rowValues.put(key,(Float) value);
        return this;
    }

    /**
     * 存储为Short类型
     * @param key
     * @param value
     * @return
     */
    public Param setShort(String key, Object value) {
        rowValues.put(key,(Short) value);
        return this;
    }

    /**
     * 存储为Byte类型
     * @param key
     * @param value
     * @return
     */
    public Param setByte(String key, Object value) {
        rowValues.put(key,(Byte) value);
        return this;
    }

    /**
     * 存储为Character类型
     * @param key
     * @param value
     * @return
     */
    public Param setCharacter(String key, Object value) {
        rowValues.put(key,(Character) value);
        return this;
    }

    /**
     * 存储为Boolean类型
     * @param key
     * @param value
     * @return
     */
    public Param setBoolean(String key, Object value) {
        rowValues.put(key,(Boolean) value);
        return this;
    }

    public Object getObject(String name) {
        return rowValues.get(name);
    }

    public String getString(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return o.toString();
    }

    public Boolean getBoolean(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return Boolean.valueOf(o.toString());
    }

    public Integer getInteger(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return Integer.valueOf(o.toString());
    }

    public Long getLong(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return Long.valueOf(o.toString());
    }

    public Float getFloat(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return Float.valueOf(o.toString());
    }

    public Double getDouble(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return Double.valueOf(o.toString());
    }

    public Timestamp getTimestamp(String name){
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return (Timestamp) o;
    }


    public Date getDate(String name) {
        Object o=rowValues.get(name);
        if(o==null) {
            return null;
        }

        return (Date) o;
    }
}
