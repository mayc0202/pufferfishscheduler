package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 数据脱敏
 */
public class Desensitization implements RuleProcessor {

    private Integer start;
    private Integer end;
    private String replaceContent;

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
        for (Object param : fieldList) {
            JSONObject obj = JSONObject.parseObject(param.toString());
            validateAndSetParams(obj);
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
        if (fieldValue != null && !"".equals(fieldValue) && !name.equals(Constants.REPLACE_CONTENT) && Integer.parseInt(fieldValue.toString()) < 0) {
            throw new KettleStepException("请校验脱敏开始位置与截止位置参数值！");
        }
        switch (name) {
            case Constants.END:
                end = fieldValue != null && !"".equals(fieldValue) ? Integer.parseInt(fieldValue.toString()) : -1;
                break;
            case Constants.START:
                start = fieldValue != null && !"".equals(fieldValue) ? Integer.parseInt(fieldValue.toString()) : 0;
                break;
            case Constants.REPLACE_CONTENT:
                replaceContent = fieldValue.toString();
                break;
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        String v = String.valueOf(value);
        int length = v.length();
        StringBuilder builder = new StringBuilder();
        if (start == 0 && end == -1) {
            for (int i = 0; i < length; i++) {
                builder.append(replaceContent);
            }
            return builder.toString();
        }
        if (start > end || start > v.length() || end > v.length()) {
            throw new KettleStepException("请校验脱敏开始位置与截止位置参数值！");
        }
        for (int i = 0; i < length; i++) {
            if (i >= start && i < end)
                builder.append(replaceContent);
            else
                builder.append(v.charAt(i));
        }
        return builder.toString();
    }
}
