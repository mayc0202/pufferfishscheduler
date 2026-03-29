package com.pufferfishscheduler.plugin.processor.Impl;


import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 字符串截取
 */
public class StringTruncation implements RuleProcessor {

    private JSONArray fieldList;
    private Integer start;
    private Integer end;

    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (!metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        JSONObject data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.FIELD_LIST)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.FIELD_LIST);
        }
        fieldList = data.getJSONArray(Constants.FIELD_LIST);
        if (fieldList != null) {
            for (Object param : fieldList) {
                JSONObject obj = JSONObject.parseObject(param.toString());
                validateAndSetParams(obj);
            }
        }
    }

    /**
     * 校验并封装参数列表
     * @param obj
     * @throws KettleStepException
     */
    private void validateAndSetParams(JSONObject obj) throws KettleStepException {
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
            switch (name) {
                case Constants.END:
                    end = Integer.parseInt(fieldValue.toString());
                    break;
                case Constants.START:
                    start = Integer.parseInt(fieldValue.toString());
                    break;
            }
        }
    }


    @Override
    public Object convert(Object value) throws KettleStepException {
        if ("".equals(value) || value == null) {
            return null;
        }
        // 如果end的值是0，默认为截至到最后
        if (end == 0) end = String.valueOf(value).length();
        // start 与 end 值校验
        if (start < 0 || end < 0 || start > String.valueOf(value).length() || end > String.valueOf(value).length() || start >= end) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.TRUNCATION_START + Constants.OR + Constants.TRUNCATION_END);
        }
        return String.valueOf(value).substring(start, end);
    }
}
