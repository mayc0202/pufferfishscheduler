package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 字母大小写转换
 */
public class LetterCapitalizationConversion implements RuleProcessor {

    private Integer convertType = 0;

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
                if (fieldValue != null && !"".equals(fieldValue)) {
                    if (name.equals(Constants.CONVERT_TYPE)) {
                        convertType = Integer.parseInt(fieldValue.toString());
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
            // 全部转大写
            if (convertType == 0) {
                result = String.valueOf(value).toUpperCase();
            }
            // 全部转小写
            if (convertType == 1) {
                result = String.valueOf(value).toLowerCase();
            }
        }
        return result;
    }
}
