package com.pufferfishscheduler.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.pufferfishscheduler.plugin.load.DelimiterParser;
import com.pufferfishscheduler.plugin.load.DorisBatchStreamLoad;
import com.pufferfishscheduler.plugin.load.DorisOptions;
import com.pufferfishscheduler.plugin.serializer.DorisRecordSerializer;
import com.pufferfishscheduler.plugin.util.*;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import static com.pufferfishscheduler.plugin.load.LoadConstants.*;


/**
 * Doris Stream Load
 */
public class DorisStreamLoader extends BaseStep implements StepInterface {
    private static Class<?> PKG = DorisStreamLoaderMeta.class; // for i18n purposes, needed by Translator2!!
    private DorisStreamLoaderMeta meta;
    private DorisStreamLoaderData data;
    private DorisBatchStreamLoad streamLoad;
    private DorisOptions options;

    public DorisStreamLoader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                             Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (DorisStreamLoaderMeta) smi;
        data = (DorisStreamLoaderData) sdi;

        try {
            Object[] r = getRow(); // Get row from input rowset & set row busy!

            if (r == null) { // no more input to be expected...

                // 如果设置了清空表数据
                if (first && meta.getTruncateTable()) {

                    // 清空表数据
                    try {
                        truncateData(meta);
                    } catch (KettleStepException e) {
                        e.printStackTrace();
                        throw new KettleException(e.getMessage());
                    }
                }

                setOutputDone();
                closeOutput();
                return false;
            }


            if (first) {
                first = false;

                // 如果设置了清空表数据
                if (meta.getTruncateTable()) {

                    // 清空表数据
                    try {
                        truncateData(meta);
                    } catch (KettleStepException e) {
                        e.printStackTrace();
                        throw new KettleException(e.getMessage());
                    }
                }
            }


            int length = meta.getFieldStream().length;
            data.keynrs = new int[length];

            Object[] row = new Object[length];

            data.formatMeta = new ValueMetaInterface[length];
            for (int i = 0; i < length; i++) {
                String filedName = meta.getFieldStream()[i];
                data.keynrs[i] = getInputRowMeta().indexOfValue(filedName);
                int keynr = data.keynrs[i];
                ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(keynr);
                data.formatMeta[i] = sourceMeta.clone();
                row[i] = r[keynr];
            }

            Properties loadProperties = options.getStreamLoadProp();
            //builder serializer
            String property = loadProperties.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT);

            data.serializer = DorisRecordSerializer.builder()
                    .setType(loadProperties.getProperty(FORMAT_KEY, CSV))
                    .setFieldNames(meta.getFieldTable())
                    .setFormatMeta(data.formatMeta)
                    .setFieldDelimiter(
                            DelimiterParser.parse(
                                    DelimiterParser.convertSeparator(property),
                                    String.valueOf('\u0001')))
                    .setLogChannelInterface(log)
                    .setDeletable(options.isDeletable())
                    .build();

            //serializer data
            streamLoad.writeRecord(meta.getDatabase(), meta.getTable(), data.serializer.serialize(row));
            putRow(getInputRowMeta(), r);
            incrementLinesOutput();

            return true;
        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "DorisStreamLoader.Log.ErrorInStep"), e);
            setErrors(1);
            stopAll();
            setOutputDone(); // signal end to receiver(s)
            return false;
        }
    }

    private void closeOutput() throws Exception {
        logDetailed("Closing output...");
        streamLoad.forceFlush();
        streamLoad.close();
        streamLoad = null;
    }

    /**
     * 清空表数据
     *
     * @param meta
     * @throws KettleStepException
     */
    private void truncateData(DorisStreamLoaderMeta meta) throws KettleStepException {

        String dbType = DatabaseConstants.transDatabseType(meta.getDatabaseType());
        String driver = JdbcUrlUtil.getDriver(dbType);
        String url = JdbcUrlUtil.getUrl(dbType, meta.getHost(), meta.getPort(), meta.getDatabase(),meta.getExtConfig());

        DbDatabaseVo dbVo = new DbDatabaseVo();
        dbVo.setType(meta.getDatabaseType());
        dbVo.setDbHost(meta.getHost());
        dbVo.setDbPort(meta.getPort());
        dbVo.setDbName(meta.getDatabase());
        dbVo.setDbSchema("");
        dbVo.setUsername(meta.getUsername());
        dbVo.setPassword(meta.getPassword());
        dbVo.setProperties(meta.getExtConfig());

        try (Connection conn = JdbcUtil.getConnection(driver, url, dbVo);
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + "`" + meta.getTable() + "`");
        } catch (Exception e) {
            e.printStackTrace();
            throw new KettleStepException("[" + meta.getTable() + "]表清空数据失败：" + e.getMessage());
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (DorisStreamLoaderMeta) smi;
        data = (DorisStreamLoaderData) sdi;

        if (super.init(smi, sdi)) {
            Properties streamHeaders = new Properties();
            StringBuffer streamLoadProp = new StringBuffer();
            String prop = meta.getStreamLoadProp();
            if (null != prop && !"".equals(prop)) {
                streamLoadProp.append(prop);
            }
            streamLoadProp.append("timeout").append(":").append(meta.getLoadTimeout());
            streamLoadProp.append(";").append("strict_mode").append(":").append("true");
            if (null != meta.getMergeType() && !"".equals(meta.getMergeType())) {
                streamLoadProp.append(";").append("merge_type").append(":").append(meta.getMergeType());
            }

            String partialColumns = meta.getPartialColumns();
            if (null != partialColumns && !"".equals(partialColumns) && "true".equals(partialColumns)) {
                streamLoadProp.append(";").append("partial_columns").append(":").append(partialColumns);
            }

            if (meta.getFieldTable() != null && !streamLoadProp.toString().contains("format:json")) {
                streamLoadProp.append(";").append("columns").append(":");
                String[] fields = meta.getFieldTable();
                for (int i = 0; i < fields.length; i++) {
                    String field = fields[i];
                    streamLoadProp.append(String.format("`%s`", field));
                    if (i < fields.length - 1) {
                        streamLoadProp.append(",");
                    }
                }
            }

            if (!streamLoadProp.toString().contains(FIELD_DELIMITER_KEY)) {
                streamLoadProp.append(";").append("column_separator").append(":").append("\\x01");
            }
            if (!streamLoadProp.toString().contains(LINE_DELIMITER_KEY)) {
                streamLoadProp.append(";").append("line_delimiter").append(":").append("\\x02");
            }

            String properties = streamLoadProp.toString();
            if (StringUtils.isNotBlank(properties)) {
                String[] keyValues = properties.split(";");

                for (String keyValue : keyValues) {
                    String[] kv = keyValue.split(":");
                    if (kv.length == 2) {
                        streamHeaders.put(kv[0], kv[1]);
                    }
                }
            }
            options = DorisOptions.builder()
                    .withFenodes(meta.getFenodes())
                    .withDatabase(meta.getDatabase())
                    .withTable(meta.getTable())
                    .withUsername(meta.getUsername())
                    .withPassword(meta.getPassword())
                    .withBufferFlushMaxBytes(meta.getBufferFlushMaxBytes())
                    .withBufferFlushMaxRows(meta.getBufferFlushMaxRows())
                    .withMaxRetries(meta.getMaxRetries())
                    .withStreamLoadProp(streamHeaders)
                    .withDeletable(meta.isDeletable())
                    .withLoadTimeout(meta.getLoadTimeout())
                    .withConnectTimeout(meta.getConnectTimeout())
                    .withScanningFrequency(meta.getScanningFrequency())
                    .build();

            logDetailed("Initializing step with options: " + options.toString());
            try {
                streamLoad = new DorisBatchStreamLoad(options, log);

                // 设置异常回调：记录错误并停止转换
                streamLoad.setExceptionHandler(e -> {
                    setErrors(1L); // 设置错误计数
                    logError("Async task error: " + e.getMessage(), e); // 记录详细错误
                    stopAll(); // 强制停止所有步骤
                });

                // 立即检查已有异常
                streamLoad.checkAsyncError();
            } catch (KettleStepException e) {
                setErrors(1L);
                logError(e.getMessage());
            }
            return true;
        }
        return false;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (DorisStreamLoaderMeta) smi;
        data = (DorisStreamLoaderData) sdi;
        // Close the output streams if still needed.
        try {
            if (streamLoad != null && streamLoad.isLoadThreadAlive()) {
                streamLoad.forceFlush();
                streamLoad.close();
                streamLoad = null;
            }
        } catch (Exception e) {
            setErrors(1L);
            logError(BaseMessages.getString(PKG, "DorisStreamLoader.Message.UNEXPECTEDERRORCLOSING"), e);
        }

        super.dispose(smi, sdi);
    }

    @VisibleForTesting
    public DorisBatchStreamLoad getStreamLoad() {
        return streamLoad;
    }
}
