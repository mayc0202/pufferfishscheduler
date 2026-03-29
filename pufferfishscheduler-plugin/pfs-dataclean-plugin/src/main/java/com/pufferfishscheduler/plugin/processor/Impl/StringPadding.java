package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 字符串填充
 */
public class StringPadding implements RuleProcessor {

    private JSONArray fieldList;
    private Integer paddingPosition;
    private String paddingContent;
    private Integer length = 0;

    @Override
    public void init(JSONObject metadata) throws KettleStepException {
        if (metadata == null || !metadata.containsKey(Constants.DATA)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.DATA);
        }
        JSONObject data = metadata.getJSONObject(Constants.DATA);
        if (!data.containsKey(Constants.FIELD_LIST)) {
            throw new KettleStepException(Constants.META_IS_NOT_CONTAIN_KEY + Constants.FIELD_LIST);
        }
        this.fieldList = data.getJSONArray(Constants.FIELD_LIST);
        if (this.fieldList != null) {
            for (Object param : this.fieldList) {
                JSONObject obj = JSONObject.parseObject(param.toString());
                validateAndSetParams(obj);
            }
        }
    }

    /**
     * 校验并封装参数列表
     *
     * @param obj
     * @throws KettleStepException
     */
    private void validateAndSetParams(JSONObject obj) throws KettleStepException {
        if (!obj.containsKey(Constants.NAME) ||
                !obj.containsKey(Constants.SELECT_ARRAY) ||
                !obj.containsKey(Constants.REQUIRES)) {
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
                case Constants.PADDING_POSITION:
                    paddingPosition = Integer.parseInt(fieldValue.toString());
                    break;
                case Constants.PADDING_CONTENT:
                    paddingContent = fieldValue.toString();
                    break;
                case Constants.PADDING_LENGTH:
                    length = Integer.parseInt(fieldValue.toString());
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        if ("".equals(value) || value == null || Constants.NULL.equals(value)) {
            return null;
        }

        return stringSplicing(paddingPosition, value.toString(), paddingContent, length);
    }

    /**
     * 拼接字符串
     *
     * @param position
     * @param value
     * @param paddingContent
     * @param length
     * @return
     */
    private String stringSplicing(Integer position, String value, String paddingContent, Integer length) {
        if (value == null || paddingContent == null || length <= 0) {
            return value;
        }

        String padding = repeatString(paddingContent, length);

        if (position == 0) {
            return padding + value;
        } else if (position == 1) {
            return value + padding;
        }
        return value;
    }

    /**
     * 字符串重复方法
     */
    private String repeatString(String str, int times) {
        if (str == null || times <= 0) {
            return "";
        }

        StringBuilder builder = new StringBuilder(str.length() * times);
        for (int i = 0; i < times; i++) {
            builder.append(str);
        }
        return builder.toString();
    }
}
