package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 舍位操作
 */
public class Rounding implements RuleProcessor {

    private Integer roundType;
    private Integer interceptNum;

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
     *
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
                case Constants.ROUND_TYPE:
                    roundType = Integer.parseInt(fieldValue.toString());
                    break;
                case Constants.INTERCEPTNUM:
                    interceptNum = Integer.parseInt(fieldValue.toString());
                    break;
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        if (value == null || "".equals(value) || Constants.NULL.equals(value)) {
            return null;
        }
        BigDecimal numericValue = new BigDecimal(value.toString());
        BigDecimal result = null;
        switch (roundType) {
            case 0:
                result = numericValue.setScale(interceptNum, RoundingMode.HALF_UP);
                break;
            case 1:
                result = numericValue.setScale(interceptNum, RoundingMode.DOWN);
                break;
            default:
                result = numericValue;
                break;
        }
        return result;
    }
}
