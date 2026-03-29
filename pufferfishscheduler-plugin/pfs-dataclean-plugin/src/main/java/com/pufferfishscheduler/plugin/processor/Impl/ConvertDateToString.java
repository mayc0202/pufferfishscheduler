package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.util.ConvertDateToStringUtil;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

import java.util.Date;

/**
 * 日期转字符串
 */
public class ConvertDateToString implements RuleProcessor {

    private Integer after; // 转换后日期格式


    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (!metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        JSONObject data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.FIELD_LIST)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.FIELD_LIST);
        }
        JSONArray fieldList = data.getJSONArray(Constants.FIELD_LIST);
        if (fieldList != null) {
            for (Object param : fieldList) {
                JSONObject obj = JSONObject.parseObject(param.toString());
                if (!obj.containsKey(Constants.NAME) || !obj.containsKey(Constants.SELECT_ARRAY) || !obj.containsKey(Constants.REQUIRES)) {
                    throw new KettleStepException(Constants.INVALID_VALUE + Constants.FIELD_LIST);
                }
                String name = obj.getString(Constants.NAME);
                Boolean requires = obj.getBoolean(Constants.REQUIRES);
                Object fieldValue = obj.get(Constants.VALUE);
                if (requires && (fieldValue == null || "".equals(fieldValue))) {
                    throw new KettleStepException(name + Constants.REQUIRES_VALUE_IS_NULL);
                }
                if (fieldValue != null) {
                    if (name.equals(Constants.CONVERT_AFTER)) {
                        after = Integer.valueOf(fieldValue.toString());
                    }
                }
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
       Object result = null;
        if ("".equals(value) || value == null || Constants.NULL.equals(value)) {
            return null;
        } else {
            Date date = (Date) value;
            result = ConvertDateToStringUtil.convertDateToString(date,after);
        }
        return result;
    }
}
