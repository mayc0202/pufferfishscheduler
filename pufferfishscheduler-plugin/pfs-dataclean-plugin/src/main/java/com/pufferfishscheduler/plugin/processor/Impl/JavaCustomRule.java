package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.custom.JavaClassBodyExecutor;
import com.pufferfishscheduler.plugin.common.custom.Param;
import com.pufferfishscheduler.plugin.common.custom.api.IJavaCheck;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;


/**
 * Java自定义规则
 */
public class JavaCustomRule implements RuleProcessor, Serializable {

    private static final long serialVersionUID = -6673029679663242922L;
    transient IClassBodyEvaluator cbe;
    IJavaCheck o;

    private Object value;
    private String javaCode;
    private Param row = new Param();
    private JavaClassBodyExecutor executor;

    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (!metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        JSONObject data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.FIELD_LIST)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.VALUE + Constants.OR + Constants.FIELD_LIST);
        }
        javaCode = String.valueOf(data.getOrDefault(Constants.JAVA_CODE, null));
        if (Constants.NULL.equals(javaCode) || "".equals(javaCode)) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.JAVA_CODE);
        }
        executor = new JavaClassBodyExecutor();
        // 1.编译java脚本
        try {
            executor.init(javaCode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new KettleStepException("Java脚本编译异常：" + e.getMessage());
        }
        // 缓存参数列表的键值对
        JSONArray fieldList = data.getJSONArray(Constants.FIELD_LIST);
        if (fieldList != null) {
            for (Object param : fieldList) {
                JSONObject obj = JSONObject.parseObject(param.toString());
                String code = obj.getString(Constants.CODE);
                Object value = obj.get(Constants.VALUE);
                row.put(code, value);
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleException, SQLException, UnsupportedEncodingException {
        Object o = executor.javaCustomRule(row, value);
        return o;
    }
}
