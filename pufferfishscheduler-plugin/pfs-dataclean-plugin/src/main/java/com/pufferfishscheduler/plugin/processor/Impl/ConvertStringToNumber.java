package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 字符串转数字
 */
public class ConvertStringToNumber implements RuleProcessor {

    // 新字段数据类型
    private String renameType;

    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (!metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        JSONObject data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.FIELD_LIST) || !data.containsKey(Constants.RENAME_TYPE)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.FIELD_LIST + Constants.OR + Constants.RENAME_TYPE);
        }
        renameType = data.getString(Constants.RENAME_TYPE);
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
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        if ("".equals(value) || value == null || Constants.NULL.equals(value)) {
            return null;
        }
        if (renameType.equals(Constants.NUMBER) || String.valueOf(value).contains(".")) {
            return Double.parseDouble(value.toString());
        } else if (renameType.equals(Constants.INTEGER)) {
            return Integer.parseInt(value.toString());
        } else {
            return Long.parseLong(value.toString());
        }
    }
}
