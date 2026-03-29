package com.pufferfishscheduler.plugin.processor;

import com.alibaba.fastjson2.JSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

/**
 * 集成平台规则处理器接口
 */
public interface RuleProcessor {

    /**
     *  解构json
     * @param metadata
     */
    void init(JSONObject metadata) throws KettleStepException;

    /**
     * 数据转换2
     * @param value
     * @return
     * @throws KettleException
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    Object convert(Object value) throws KettleException, SQLException, UnsupportedEncodingException;

}
