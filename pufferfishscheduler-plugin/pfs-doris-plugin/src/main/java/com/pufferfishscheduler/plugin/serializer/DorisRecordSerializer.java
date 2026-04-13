package com.pufferfishscheduler.plugin.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.pufferfishscheduler.plugin.load.EscapeHandler;
import com.pufferfishscheduler.plugin.load.LoadConstants;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;


/** Serializer for RowData. */
public class DorisRecordSerializer {
    String[] fieldNames;
    String type;
    private ObjectMapper objectMapper;
    private final String fieldDelimiter;
    private final ValueMetaInterface[] formatMeta;
    private LogChannelInterface log;
    private final boolean deletable;

    private DorisRecordSerializer(
            String[] fieldNames,
            ValueMetaInterface[] formatMeta,
            String type,
            String fieldDelimiter,
            LogChannelInterface log,
            boolean deletable) {
        this.fieldNames = fieldNames;
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        if (LoadConstants.JSON.equals(type)) {
            objectMapper = new ObjectMapper();
        }
        this.formatMeta = formatMeta;
        this.log = log;
        this.deletable = deletable;
    }


    public byte[] serialize(Object[] record) throws IOException, KettleException {
        int maxIndex = Math.min(record.length, fieldNames.length);
        String valString;
        if (LoadConstants.JSON.equals(type)) {
            valString = buildJsonString(record, maxIndex);
        } else if (LoadConstants.CSV.equals(type)) {
            valString = buildCSVString(record, maxIndex);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
        log.logRowlevel("Serialized record: " + valString);
        return valString.getBytes(StandardCharsets.UTF_8);
    }


    public String buildJsonString(Object[] record, int maxIndex) throws IOException, KettleException {
        int fieldIndex = 0;
        Map<String, String> valueMap = new HashMap<>();
        while (fieldIndex < maxIndex) {
            Object field = convertExternal(record[fieldIndex], formatMeta[fieldIndex]);
            String value = field != null ? field.toString() : null;
            valueMap.put(fieldNames[fieldIndex], value);
            fieldIndex++;
        }
        if (deletable) {
            // All load data will be deleted
            valueMap.put(LoadConstants.DORIS_DELETE_SIGN, "1");
        }
        return objectMapper.writeValueAsString(valueMap);
    }

    public String buildCSVString(Object[] record, int maxIndex) throws IOException, KettleException {
        int fieldIndex = 0;
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        while (fieldIndex < maxIndex) {
            Object field = convertExternal(record[fieldIndex], formatMeta[fieldIndex]);
            String value = field != null ? field.toString() : LoadConstants.NULL_VALUE;
            joiner.add(value);
            fieldIndex++;
        }
        if (deletable) {
            // All load data will be deleted
            joiner.add("1");
        }
        return joiner.toString();
    }

    private Object convertExternal(Object r, ValueMetaInterface sourceMeta) throws KettleException {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_BOOLEAN:
                    return sourceMeta.getBoolean(r);
                case ValueMetaInterface.TYPE_INTEGER:
                    return sourceMeta.getInteger(r);
                case ValueMetaInterface.TYPE_NUMBER:
                    return sourceMeta.getNumber(r);
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    return sourceMeta.getBigNumber(r);
                case ValueMetaInterface.TYPE_DATE:
//                    return sourceMeta.getDate(r);
                    Date dateValue = sourceMeta.getDate(r);
                    // 检查是否包含时间部分
                    if (hasTimeComponent(dateValue)) {
                        // 包含时分秒，返回 Timestamp
                        return new Timestamp(dateValue.getTime());
                    } else {
                        // 纯日期，返回 Date
                        return new java.sql.Date(dateValue.getTime());
                    }
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    return (Timestamp) sourceMeta.getDate(r);
                case ValueMetaInterface.TYPE_BINARY:
                case ValueMetaInterface.TYPE_STRING:
                    return sourceMeta.getString(r);
                default:
                    // Unknow type, use origin value
                    return r;
            }
        } catch (Exception e) {
            throw new KettleException("Error serializing rows of data to the Doris: ", e);
        }
    }

    /**
     * 检查日期是否包含时间部分（非00:00:00.000）
     */
    private boolean hasTimeComponent(Date date) {
        if (date == null) return false;

        // 方法1：使用Calendar检查（兼容旧Java版本）
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        return cal.get(Calendar.HOUR_OF_DAY) != 0 ||
                cal.get(Calendar.MINUTE) != 0 ||
                cal.get(Calendar.SECOND) != 0 ||
                cal.get(Calendar.MILLISECOND) != 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for RowDataSerializer. */
    public static class Builder {
        private String[] fieldNames;
        private ValueMetaInterface[] formatMeta;
        private String type;
        private String fieldDelimiter;
        private LogChannelInterface log;
        private boolean deletable;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFormatMeta(ValueMetaInterface[] formatMeta) {
            this.formatMeta = formatMeta;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = EscapeHandler.escapeString(fieldDelimiter);
            return this;
        }

        public Builder setLogChannelInterface(LogChannelInterface log) {
            this.log = log;
            return this;
        }

        public Builder setDeletable(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public DorisRecordSerializer build() {
            Preconditions.checkState(
                    LoadConstants.CSV.equals(type) && fieldDelimiter != null
                            || LoadConstants.JSON.equals(type));
            Preconditions.checkNotNull(formatMeta);
            Preconditions.checkNotNull(fieldNames);

            return new DorisRecordSerializer(fieldNames, formatMeta, type, fieldDelimiter, log, deletable);
        }
    }
}
