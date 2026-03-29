package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.util.ConvertDateToStringUtil;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 日期转时间戳
 */
public class ConvertDateToTimeStamp implements RuleProcessor {

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
            }
        }
    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        Object result = null;
        if ("".equals(value) || Constants.NULL.equals(value) || value == null) {
            return null;
        } else {
            String data = String.valueOf(value).replaceAll("'", "").replaceAll("\"", "");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.DATE_FORMAT, Locale.US);
            Date date = null;
            try {
                date = simpleDateFormat.parse(data);
            } catch (ParseException e) {
                throw new KettleStepException(Constants.VALUE + Constants.ANALYSIS_EXCEPTION + e.getMessage());
            }
            result = ConvertDateToStringUtil.convertDateToTimeStamp(date);
        }
        return result;
    }
}
