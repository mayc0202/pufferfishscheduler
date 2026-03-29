package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.util.ConvertDateToStringUtil;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

/**
 * 字符串转日期
 */
public class ConvertStringToDate implements RuleProcessor {

    private Integer before;

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
                // TODO 调整参数命名，符合规范
                case Constants.CONVERT_BEFORE:
                    before = Integer.parseInt(fieldValue.toString());
                    break;
            }
        }
    }


    @Override
    public Object convert(Object value) throws KettleStepException {
        Object result = null;
        if ("".equals(value) || value == null || Constants.NULL.equals(value)) {
            return null;
        } else {
            result = ConvertDateToStringUtil.convertStringToDate(String.valueOf(value),before);
        }
        return result;
    }

}
