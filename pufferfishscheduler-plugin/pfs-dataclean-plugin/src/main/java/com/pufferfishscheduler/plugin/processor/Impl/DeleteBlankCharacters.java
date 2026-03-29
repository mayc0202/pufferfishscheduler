package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 删除空白字符
 */
public class DeleteBlankCharacters implements RuleProcessor {

    private Integer deleteType;
    private Integer deleteContent;

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
                case Constants.DELETE_TYPE:
                    deleteType = Integer.valueOf(fieldValue.toString());
                    break;
                case Constants.DELETE_CONTENT:
                    deleteContent = Integer.valueOf(fieldValue.toString());
                    break;
            }
        }
    }


    @Override
    public Object convert(Object value) throws KettleStepException {
        Object result = null;
        if (deleteType == 0 && deleteContent == 0) {
            // 去除首尾空白字符串(包括空格、制表符、换行符等)
            result = String.valueOf(value).trim();
        } else if (deleteType == 0 && deleteContent == 1) {
            // 去除首尾空格
            result = String.valueOf(value).replaceFirst("^ +", "").replaceFirst(" +$", "");
        } else if (deleteType == 1 && deleteContent == 0) {
            // 去除所有空白字符
            result = String.valueOf(value).replaceAll("\\s", "");
        } else if (deleteType == 1 && deleteContent == 1) {
            // 去除所有空格
            result = String.valueOf(value).replaceAll(" ", "");
        }
        return result;
    }

    /**
     * 删除空格不删除空白字符
     *
     * @param field
     * @return
     */
    String dealStr(String field) {
        if (field == null || field.equals("")) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        boolean flag = true;
        boolean start = true;
        boolean after = true;
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if ((i == 0 && c == ' ') || (i == field.length() - 1 && c == ' ')) {
                flag = false;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }
}
