package com.pufferfishscheduler.plugin;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Param;
import com.pufferfishscheduler.plugin.common.WayType;
import org.apache.commons.compress.utils.Lists;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API输出步骤 - 用于调用HTTP API输出数据
 */
public class ApiOutputStep extends BaseStep implements StepInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiOutputStep.class);
    private static final String PAGE_NO_PLACEHOLDER = "${page_no}";
    private static final String VARIABLE_PREFIX = "${";
    private static final String VARIABLE_SUFFIX = "}";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final int EXTRA_FIELDS_COUNT = 3;
    private static final int STATUS_CODE_INDEX = 0;
    private static final int HEADERS_INDEX = 1;
    private static final int BODY_INDEX = 2;

    private static CloseableHttpClient httpClient;
    private static RequestConfig requestConfig;

    private ApiOutputStepMeta meta;
    private ApiOutputStepData data;

    public ApiOutputStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                         int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }

        this.meta = (ApiOutputStepMeta) smi;
        this.data = (ApiOutputStepData) sdi;
        this.data.standaloneRunCompleted = false;

        initializeHttpClient();
        initializeRequestConfig();

        logDebug("API Output Step initialized successfully");
        return true;
    }

    private void initializeHttpClient() {
        if (httpClient != null) {
            return;
        }

        try {
            SSLContext sslContext = createTrustAllSslContext();
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(
                    sslContext, NoopHostnameVerifier.INSTANCE);
            httpClient = HttpClients.custom()
                    .setSSLSocketFactory(socketFactory)
                    .build();
        } catch (Exception e) {
            LOGGER.warn("Failed to create custom SSL HTTP client, using default", e);
            httpClient = HttpClients.createDefault();
        }
    }

    private SSLContext createTrustAllSslContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        X509TrustManager trustAllManager = new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                // Trust all clients
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                // Trust all servers
            }
        };
        sslContext.init(null, new TrustManager[]{trustAllManager}, null);
        return sslContext;
    }

    private void initializeRequestConfig() {
        requestConfig = RequestConfig.custom()
                .setConnectTimeout(meta.getConnectOutTime())
                .setConnectionRequestTimeout(meta.getReadOutTime())
                .setSocketTimeout(meta.getConnectOutTime())
                .build();
    }

    /**
     * 处理数据行
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
            throws KettleException, KettleStepException {

        Object[] currentRow = getRow();

        if (isFirstRow()) {
            initializeFirstRow();
        }

        if (shouldStopProcessing(currentRow)) {
            setOutputDone();
            return false;
        }

        executeRequest(currentRow, 1L);

        if (isStandaloneInput()) {
            data.standaloneRunCompleted = true;
        }
        return true;
    }

    private boolean isFirstRow() {
        return first;
    }

    /**
     * 初始化第一行数据
     */
    private void initializeFirstRow() throws KettleStepException {
        RowMetaInterface inputMeta = getInputRowMeta();
        if (inputMeta == null) {
            data.inputRowMeta = new RowMeta();
        } else {
            data.inputRowMeta = inputMeta;
        }
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields(data.outputRowMeta, getStepname(), null, null,
                this, repository, metaStore);
        first = false;
    }

    /**
     * 判断是否应该停止处理
     */
    private boolean shouldStopProcessing(Object[] currentRow) {
        if (data.standaloneRunCompleted) {
            return true;
        }
        if (isStandaloneInput()) {
            return false;
        }
        if (currentRow == null) {
            setInputRowMeta(new RowMeta());
            return true;
        }
        return false;
    }

    private boolean isStandaloneInput() {
        StepMeta[] prev = getTransMeta().getPrevSteps(getStepMeta());
        return prev == null || prev.length == 0;
    }

    /**
     * 执行请求
     */
    private void executeRequest(Object[] currentRow, Long pageNo) throws KettleException {
        String url = buildRequestUrl(currentRow, pageNo);

        if (WayType.GET.getDescription().equals(meta.getRequestMethod())) {
            executeGetRequest(currentRow, pageNo, url);
        } else {
            executePostRequest(currentRow, pageNo, url);
        }
    }

    /**
     * 构建请求URL
     */
    private String buildRequestUrl(Object[] currentRow, Long pageNo) {
        return getUrlValue(meta.getUrl(), currentRow, pageNo);
    }

    private void executeGetRequest(Object[] currentRow, Long pageNo, String url)
            throws KettleException {
        executeWithRetry(() -> {
            String finalUrl = url;
            if (meta.isUseXForm()) {
                finalUrl = appendXFormsToUrl(url, pageNo, currentRow);
            }

            HttpGet httpGet = new HttpGet(finalUrl);
            httpGet.setConfig(requestConfig);
            addHeaders(currentRow, httpGet, pageNo);

            return httpGet;
        }, currentRow);
    }

    /**
     * 执行POST请求
     */
    private void executePostRequest(Object[] currentRow, Long pageNo, String url)
            throws KettleException {
        executeWithRetry(() -> {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            addHeaders(currentRow, httpPost, pageNo);

            if (meta.isUseXForm()) {
                addPostXForms(pageNo, httpPost, currentRow);
            } else if (meta.isUseRaw()) {
                addPostRawBody(pageNo, httpPost, currentRow);
            }

            return httpPost;
        }, currentRow);
    }

    @FunctionalInterface
    private interface RequestBuilder {
        HttpUriRequest build() throws Exception;
    }

    /**
     * 执行请求并重试
     */
    private void executeWithRetry(RequestBuilder requestBuilder, Object[] currentRow)
            throws KettleException {
        int maxRetries = meta.getRetryNum();
        long retryDelayMs = meta.getRetryTime();
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            HttpUriRequest request = null;
            try {
                request = requestBuilder.build();
                HttpResponse response = httpClient.execute(request);
                processResponse(response, currentRow);
                return; // 成功则返回

            } catch (Exception e) {
                lastException = e;
                releaseRequest(request);

                if (attempt >= maxRetries) {
                    handleFinalFailure(e);
                } else {
                    logRetryAttempt(attempt + 1, request, retryDelayMs);
                    sleepSafely(retryDelayMs);
                }
            }
        }

        throw new KettleException("API call failed after " + maxRetries + " retries", lastException);
    }

    private void releaseRequest(HttpUriRequest request) {
        if (request != null) {
            try {
                request.abort();
            } catch (Exception e) {
                LOGGER.debug("Failed to release request", e);
            }
        }
    }

    private void processResponse(HttpResponse response, Object[] currentRow) throws IOException, KettleStepException {
        Object[] resultRow = buildResultRow(response, currentRow);
        putRow(data.outputRowMeta, resultRow);
        incrementLinesOutput();
    }

    private Object[] buildResultRow(HttpResponse response, Object[] currentRow) throws IOException {
        HttpEntity entity = response.getEntity();

        Object[] extraFields = new Object[EXTRA_FIELDS_COUNT];
        extraFields[STATUS_CODE_INDEX] = (long) response.getStatusLine().getStatusCode();
        extraFields[HEADERS_INDEX] = JSONObject.toJSONString(response.getAllHeaders());
        extraFields[BODY_INDEX] = EntityUtils.toString(entity, meta.getResponseCode());

        Object[] result = resizeArray(currentRow, data.inputRowMeta.size() + extraFields.length);
        System.arraycopy(extraFields, 0, result, data.inputRowMeta.size(), extraFields.length);

        return result;
    }

    private void logRetryAttempt(int attemptNumber, HttpUriRequest request, long delayMs) {
        String url = request != null ? request.getURI().toString() : "unknown";
        long delaySeconds = TimeUnit.MILLISECONDS.toSeconds(delayMs);
        logBasic(String.format("API call failed: %s, retry %d after %d seconds",
                url, attemptNumber, delaySeconds));
    }

    private void sleepSafely(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Retry delay interrupted", e);
        }
    }

    private void handleFinalFailure(Exception e) throws KettleException {
        logError("API call failed after all retries", e);
        setErrors(1L);
        stopAll();
        throw new KettleException(e);
    }

    private void addPostXForms(Long pageNo, HttpPost httpPost, Object[] currentRow)
            throws UnsupportedEncodingException {
        List<Param> xforms = meta.getxFormParams();
        if (xforms == null || xforms.isEmpty()) {
            return;
        }

        List<NameValuePair> params = Lists.newArrayList();
        for (Param param : xforms) {
            String value = environmentSubstitute(
                    String.valueOf(getValue(param.getValue(), currentRow, pageNo)));
            params.add(new BasicNameValuePair(param.getName(), value));
        }

        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, meta.getRequestCode());
        httpPost.setEntity(entity);
    }

    private void addPostRawBody(Long pageNo, HttpPost httpPost, Object[] currentRow) {
        String rawBody = meta.getRaw();
        Object processedValue = getValue(rawBody, currentRow, pageNo);
        String content = environmentSubstitute(String.valueOf(processedValue));

        StringEntity entity = new StringEntity(content, meta.getRequestCode());
        entity.setContentType(meta.getRequestType());
        entity.setContentEncoding(new BasicHeader("Content-Encoding", meta.getRequestCode()));
        httpPost.setEntity(entity);
    }

    private void addHeaders(Object[] currentRow, HttpUriRequest request, Long pageNo) {
        List<Param> headers = meta.getParams();
        boolean hasContentType = false;

        if (headers != null && !headers.isEmpty()) {
            for (Param header : headers) {
                String value = environmentSubstitute(
                        String.valueOf(getValue(header.getValue(), currentRow, pageNo)));
                request.addHeader(header.getName(), value);

                if (CONTENT_TYPE_HEADER.equalsIgnoreCase(header.getName())) {
                    hasContentType = true;
                }
            }
        }

        // 如果是POST请求且没有设置Content-Type，添加默认值
        if (!hasContentType && request instanceof HttpPost) {
            request.addHeader(CONTENT_TYPE_HEADER, meta.getRequestType());
        }
    }

    private String appendXFormsToUrl(String baseUrl, Long pageNo, Object[] currentRow)
            throws UnsupportedEncodingException {
        List<Param> xforms = meta.getxFormParams();
        if (xforms == null || xforms.isEmpty()) {
            return baseUrl;
        }

        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        String separator = baseUrl.contains("?") ? "&" : "?";

        for (Param xform : xforms) {
            urlBuilder.append(separator)
                    .append(xform.getName())
                    .append("=");

            String value = environmentSubstitute(
                    String.valueOf(getValue(xform.getValue(), currentRow, pageNo)));
            urlBuilder.append(value);

            separator = "&";
        }

        return urlBuilder.toString();
    }

    public static Object[] resizeArray(Object[] original, int newSize) {
        if (original != null && original.length >= newSize) {
            return original;
        }

        Object[] newArray = new Object[newSize + RowDataUtil.OVER_ALLOCATE_SIZE];
        if (original != null) {
            System.arraycopy(original, 0, newArray, 0, original.length);
        }
        return newArray;
    }

    public String getUrlValue(String template, Object[] row, Long pageNo) {
        if (template == null) {
            return "";
        }

        String result = replacePageNoPlaceholder(template, pageNo);
        result = replaceRowVariables(result, row);

        return environmentSubstitute(result);
    }

    public Object getValue(String template, Object[] row, Long pageNo) {
        if (template == null) {
            return "";
        }

        String result = replacePageNoPlaceholder(template, pageNo);
        result = replaceRowVariables(result, row);

        return result;
    }

    private String replacePageNoPlaceholder(String template, Long pageNo) {
        if (template.contains(PAGE_NO_PLACEHOLDER)) {
            return template.replace(PAGE_NO_PLACEHOLDER, String.valueOf(pageNo));
        }
        return template;
    }

    private String replaceRowVariables(String template, Object[] row) {
        if (row == null) {
            return template;
        }

        String result = template;
        List<RowSet> inputRowSets = getInputRowSets();

        for (RowSet rowSet : inputRowSets) {
            RowMetaInterface rowMeta = rowSet.getRowMeta();
            if (rowMeta != null && rowMeta.size() > 0) {
                for (int i = 0; i < rowMeta.size() && i < row.length; i++) {
                    ValueMetaInterface valueMeta = rowMeta.getValueMeta(i);
                    String placeholder = VARIABLE_PREFIX + valueMeta.getName() + VARIABLE_SUFFIX;

                    if (result.contains(placeholder) && row[i] != null) {
                        result = result.replace(placeholder, row[i].toString());
                    }
                }
            }
        }

        return result;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = (ApiOutputStepMeta) smi;
        this.data = (ApiOutputStepData) sdi;

        super.dispose(smi, sdi);
        logDebug("API Output Step disposed");
    }
}