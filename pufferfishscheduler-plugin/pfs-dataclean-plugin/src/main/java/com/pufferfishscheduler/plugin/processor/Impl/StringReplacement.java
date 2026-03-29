package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;


/**
 * 字符串替换
 */
public class StringReplacement implements RuleProcessor {

    private JSONArray fieldList;
    private String replaceContent; // 替换内容
    private Integer replaceType; // 替换方式
    private Integer start;
    private Integer end;
    private String searchContent; // 查找内容

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
                case Constants.SEARCH_CONTENT:
                    searchContent = fieldValue.toString();
                    break;
                case Constants.REPLACE_CONTENT:
                    replaceContent = fieldValue.toString();
                    break;
                case Constants.REPLACE_TYPE:
                    replaceType = Integer.parseInt(fieldValue.toString());
                    break;
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
        if ("".equals(value) || value == null || Constants.NULL.equals(value)) {
            return null;
        }

        if (replaceType == 0) {
            return dealReplaceTyep1(value.toString(), replaceContent);
        } else {
            return dealReplaceTyep2(value.toString(), replaceContent);
        }
    }

    /**
     * 处理字符串替换1
     *
     * @param value
     * @param replaceContent
     * @return
     */
    private String dealReplaceTyep1(String value, String replaceContent) throws KettleStepException {
        if (searchContent == null) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.SEARCH_CONTENT);
        }
        return value.replaceAll(searchContent, replaceContent);
    }

    /**
     * 处理字符串替换2
     *
     * @param value
     * @param replaceContent
     * @return
     * @throws KettleStepException
     */
    private String dealReplaceTyep2(String value, String replaceContent) throws KettleStepException {
        // 参数校验：确保 start 和 end 合法
        if (start < 0 || end < 0 || start > end) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.START + Constants.OR + Constants.END);
        }

        // 若 end=0 则默认替换到字符串末尾
        if (end == 0) {
            end = value.length();
        }

        // 超出字符串长度时调整或抛异常
        if (start > value.length()) {
            throw new KettleStepException(Constants.INVALID_VALUE + Constants.START);
        }

        // 确保 end 不超过字符串长度
        end = Math.min(end, value.length());

        StringBuilder builder = new StringBuilder();

        // 添加 start 之前的子串
        builder.append(value, 0, start);

        // 添加替换内容
        for (int i = start; i <= end; i++) {
            builder.append(replaceContent);
        }

        // 添加 end 之后的子串
        builder.append(value, end, value.length());

        return builder.toString();
    }
}
