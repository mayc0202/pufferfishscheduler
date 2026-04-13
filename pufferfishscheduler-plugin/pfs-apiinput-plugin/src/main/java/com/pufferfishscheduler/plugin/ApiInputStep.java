package com.pufferfishscheduler.plugin;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Param;
import com.pufferfishscheduler.plugin.common.WayType;
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
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
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
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * API输入步骤 - 用于从HTTP API获取数据
 */
public class ApiInputStep extends BaseStep implements StepInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiInputStep.class);
    private static final String PAGE_NO_VARIABLE = "page_no";
    private static final String PAGE_NO_PLACEHOLDER = "${page_no}";
    private static final String VARIABLE_PREFIX = "${";
    private static final String VARIABLE_SUFFIX = "}";
    private static final long MAX_PAGE_LIMIT = 1_000_000L;
    private static final int EXTRA_FIELDS_COUNT = 3;
    private static final int PAGE_LOG_INTERVAL = 100;

    private static CloseableHttpClient httpClient;
    private static RequestConfig requestConfig;

    private ApiInputStepMeta meta;
    private ApiInputStepData data;
    private Expression pageConditionExpression;
    private SpelExpressionParser spelParser;

    public ApiInputStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                        int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }

        this.meta = (ApiInputStepMeta) smi;
        this.data = (ApiInputStepData) sdi;
        this.data.standaloneRunCompleted = false;

        initializeHttpClient();
        initializeRequestConfig();
        initializeSpelParser();

        logDebug("API Step initialized successfully");
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
                .setConnectTimeout(meta.getConnectOutTime().intValue())
                .setConnectionRequestTimeout(meta.getReadOutTime().intValue())
                .setSocketTimeout(meta.getConnectOutTime().intValue())
                .build();
    }

    private void initializeSpelParser() {
        if (meta.isUsePage()) {
            this.spelParser = new SpelExpressionParser();
            String condition = environmentSubstitute(replaceVariableNames(meta.getPageCondition()));
            this.pageConditionExpression = spelParser.parseExpression(condition);
        }
    }

    /**
     * Process a row of data from the input stream.
     *
     * @param smi The step meta interface.
     * @param sdi The step data interface.
     * @return True if the step should continue processing rows, false otherwise.
     * @throws KettleException If an error occurs.
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        try {
            Object[] currentRow = getRow();

            if (isFirstRow()) {
                initializeFirstRow(currentRow);
            }

            if (shouldStopProcessing(currentRow)) {
                setOutputDone();
                return false;
            }

            Long pageNo = determineStartPageNumber();

            if (meta.isUsePage()) {
                processPaginatedData(currentRow, pageNo);
            } else {
                processSinglePageData(currentRow, pageNo);
            }

            if (isStandaloneInput()) {
                data.standaloneRunCompleted = true;
            }
        } catch (Exception e) {
            handleRequestError(e);
        }

        return true;
    }

    private boolean isFirstRow() {
        return first;
    }

    private void initializeFirstRow(Object[] currentRow) {
        RowMetaInterface inputMeta = getInputRowMeta();
        if (inputMeta == null) {
            data.inputRowMeta = new RowMeta();
        } else {
            data.inputRowMeta = inputMeta;
        }
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        first = false;
    }

    private boolean shouldStopProcessing(Object[] currentRow) {
        if (data.standaloneRunCompleted) {
            return true;
        }
        if (isStandaloneInput()) {
            return false;
        }
        return !first && currentRow == null;
    }

    /** 转换中无任何上游步骤连入时，getRow() 恒为 null，且 getInputRowMeta() 可能为 null */
    private boolean isStandaloneInput() {
        StepMeta[] prev = getTransMeta().getPrevSteps(getStepMeta());
        return prev == null || prev.length == 0;
    }

    private Long determineStartPageNumber() {
        Long startPage = meta.getStartPageNo();
        return (startPage != null && startPage > 0) ? startPage : 1L;
    }

    private void processPaginatedData(Object[] currentRow, Long pageNo) throws KettleException, UnsupportedEncodingException {
        while (!isStopped()) {
            logPageProgress(pageNo);

            applyRequestDelay();

            boolean shouldContinue = executeRequest(currentRow, pageNo);

            if (shouldStopPagination(pageNo, shouldContinue)) {
                break;
            }

            pageNo++;
        }
    }

    private void logPageProgress(Long pageNo) {
        if (pageNo % PAGE_LOG_INTERVAL == 0) {
            logBasic(String.format("Currently collected page %s", pageNo));
        }
    }

    private void applyRequestDelay() {
        Long intervalFrom = meta.getIntervalFrom();
        Long intervalTo = meta.getIntervalTo();

        if (intervalTo != 0 && intervalTo > intervalFrom) {
            long delay = intervalFrom + (long) (Math.random() * (intervalTo - intervalFrom));
            try {
                TimeUnit.MILLISECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Request delay interrupted", e);
            }
        }
    }

    private boolean shouldStopPagination(Long pageNo, boolean shouldContinue) {
        return pageNo > MAX_PAGE_LIMIT || !shouldContinue || getErrors() > 0;
    }

    private void processSinglePageData(Object[] currentRow, Long pageNo) throws KettleException, UnsupportedEncodingException {
        executeRequest(currentRow, pageNo);
    }

    private boolean executeRequest(Object[] currentRow, Long pageNo) throws KettleException, UnsupportedEncodingException {
        String url = buildRequestUrl(currentRow, pageNo);

        if (WayType.GET.getDescription().equals(meta.getRequestMethod())) {
            return executeGetRequest(currentRow, pageNo, url);
        } else {
            return executePostRequest(currentRow, pageNo, url);
        }
    }

    private String buildRequestUrl(Object[] currentRow, Long pageNo) {
        return getUrlValue(meta.getUrl(), currentRow, pageNo);
    }

    private boolean executeGetRequest(Object[] currentRow, Long pageNo, String url) throws KettleException {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setConfig(requestConfig);
        addGetHeaders(currentRow, httpGet, pageNo);

        return executeHttpRequest(httpGet, currentRow, pageNo);
    }

    private boolean executePostRequest(Object[] currentRow, Long pageNo, String url) throws KettleException, UnsupportedEncodingException {
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        addPostHeaders(currentRow, httpPost, pageNo);

        if (hasXFormParams()) {
            addPostXForms(pageNo, httpPost, currentRow);
        } else {
            addPostRawBody(pageNo, httpPost, currentRow);
        }

        return executeHttpRequest(httpPost, currentRow, pageNo);
    }

    private boolean hasXFormParams() {
        List<Param> xforms = meta.getxFormParams();
        return xforms != null && !xforms.isEmpty();
    }

    private boolean executeHttpRequest(HttpUriRequest request, Object[] currentRow, Long pageNo)
            throws KettleException {
        try {
            HttpResponse response = httpClient.execute(request);
            return handleResponse(response, currentRow, pageNo);
        } catch (Exception e) {
            handleRequestError(e);
            return false;
        }
    }

    /**
     * Handle the HTTP response from the API call.
     *
     * @param response The HTTP response.
     * @param currentRow The current row of data.
     * @param pageNo The current page number.
     * @return True if the page condition is met, false otherwise.
     * @throws KettleException If the evaluation fails.
     * @throws IOException If an I/O error occurs.
     */
    private boolean handleResponse(HttpResponse response, Object[] currentRow, Long pageNo)
            throws KettleException, IOException {
        Object[] resultRow = buildResultRow(response, currentRow);

        if (meta.isUsePage()) {
            return evaluatePageCondition(resultRow, pageNo);
        } else {
            putRow(data.outputRowMeta, resultRow);
            return true;
        }
    }

    /**
     * Build the result row of data from the HTTP response.
     *
     * @param response The HTTP response.
     * @param currentRow The current row of data.
     * @return The result row of data.
     * @throws IOException If an I/O error occurs.
     */
    private Object[] buildResultRow(HttpResponse response, Object[] currentRow) throws IOException {
        HttpEntity entity = response.getEntity();
        String responseBody = EntityUtils.toString(entity, meta.getResponseCode());
        String headersJson = JSONObject.toJSONString(response.getAllHeaders());

        Object[] extraFields = {
                (long) response.getStatusLine().getStatusCode(),
                headersJson,
                responseBody
        };

        Object[] result = resizeArray(currentRow, data.inputRowMeta.size() + extraFields.length);
        System.arraycopy(extraFields, 0, result, data.inputRowMeta.size(), extraFields.length);

        return result;
    }

    /**
     * Evaluate the page condition expression for the given row of data.
     *
     * @param resultRow The result row of data.
     * @param pageNo The current page number.
     * @return True if the page condition is met, false otherwise.
     * @throws KettleException If the evaluation fails.
     */
    private boolean evaluatePageCondition(Object[] resultRow, Long pageNo) throws KettleException {
        try {
            StandardEvaluationContext context = createEvaluationContext(resultRow, pageNo);
            Object conditionResult = pageConditionExpression.getValue(context);

            if (conditionResult instanceof Boolean) {
                boolean shouldStop = (Boolean) conditionResult;
                if (!shouldStop) {
                    putRow(data.outputRowMeta, resultRow);
                }
                return shouldStop;
            }

            throw new KettleException("Filter expression must return boolean, got: " +
                    conditionResult.getClass().getName());
        } catch (Exception e) {
            throw new KettleValueException("Failed to evaluate page condition", e);
        }
    }

    /**
     * Create an evaluation context for the given row of data.
     *
     * @param row The row of data.
     * @param pageNo The current page number.
     * @return The evaluation context.
     * @throws KettleValueException If a value conversion error occurs.
     */
    private StandardEvaluationContext createEvaluationContext(Object[] row, Long pageNo)
            throws KettleValueException {
        StandardEvaluationContext context = new StandardEvaluationContext();

        for (int i = 0; i < data.outputRowMeta.size(); i++) {
            ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
            Object convertedValue = valueMeta.convertToNormalStorageType(row[i]);
            context.setVariable(valueMeta.getName(), convertedValue);
        }

        context.setVariable(PAGE_NO_VARIABLE, pageNo);
        return context;
    }

    /**
     * Handle errors that occur during API requests.
     * Log the error and set the step status to failed.
     *
     * @param e The exception that occurred.
     */
    private void handleRequestError(Exception e) {
        logError("API call failed", e);
        setErrors(1L);
        stopAll();
    }

    /**
     * Add x-form parameters to the HTTP POST request.
     *
     * @param pageNo The current page number.
     * @param httpPost The HTTP POST request.
     * @param currentRow The current row of data.
     * @throws UnsupportedEncodingException If the character encoding is not supported.
     */
    private void addPostXForms(Long pageNo, HttpPost httpPost, Object[] currentRow)
            throws UnsupportedEncodingException {
        List<Param> xforms = meta.getxFormParams();
        List<NameValuePair> params = new ArrayList<>();

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
        entity.setContentEncoding(meta.getRequestCode());
        httpPost.setEntity(entity);
    }

    private void addPostHeaders(Object[] currentRow, HttpPost httpPost, Long pageNo) {
        addHeadersCommon(currentRow, httpPost, pageNo);
    }

    private void addGetHeaders(Object[] currentRow, HttpGet httpGet, Long pageNo) {
        addHeadersCommon(currentRow, httpGet, pageNo);
    }

    private void addHeadersCommon(Object[] currentRow, HttpUriRequest request, Long pageNo) {
        List<Param> headers = meta.getParams();
        boolean hasContentType = false;

        if (headers != null && !headers.isEmpty()) {
            for (Param header : headers) {
                String value = environmentSubstitute(
                        String.valueOf(getValue(header.getValue(), currentRow, pageNo)));
                request.addHeader(header.getName(), value);

                if ("Content-Type".equalsIgnoreCase(header.getName())) {
                    hasContentType = true;
                }
            }
        }

        if (!hasContentType && request instanceof HttpPost) {
            request.addHeader("Content-Type", meta.getRequestType());
        }
    }

    /**
     * 替换变量占位符为SpEL表达式格式
     */
    private String replaceVariableNames(String input) {
        if (input == null) {
            return "";
        }

        String result = input.replace(PAGE_NO_PLACEHOLDER, "#" + PAGE_NO_VARIABLE);

        for (int i = 0; i < data.outputRowMeta.size(); i++) {
            ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
            String fieldName = valueMeta.getName();
            String placeholder = VARIABLE_PREFIX + fieldName + VARIABLE_SUFFIX;

            if (result.contains(placeholder)) {
                result = result.replace(placeholder, "#" + fieldName);
            }
        }

        return result;
    }

    public String getUrlValue(String template, Object[] row, Long pageNo) {
        if (template == null) {
            return "";
        }

        String result = template.replace(PAGE_NO_PLACEHOLDER, String.valueOf(pageNo));
        result = replaceRowVariables(result, row);

        return environmentSubstitute(result);
    }

    public Object getValue(String template, Object[] row, Long pageNo) {
        if (template == null) {
            return "";
        }

        String result = template.replace(PAGE_NO_PLACEHOLDER, String.valueOf(pageNo));
        result = replaceRowVariables(result, row);

        return result;
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

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = (ApiInputStepMeta) smi;
        this.data = (ApiInputStepData) sdi;

        super.dispose(smi, sdi);
        logDebug("API Step disposed");
    }
}