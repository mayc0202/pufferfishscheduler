package com.pufferfishscheduler.plugin;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.pufferfishscheduler.plugin.common.Param;
import com.pufferfishscheduler.plugin.common.WayType;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
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
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
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
 * 文件下载步骤 - 用于从HTTP API下载文件
 */
public class FileDownloadStep extends BaseStep implements StepInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloadStep.class);
    private static final String PAGE_NO_PLACEHOLDER = "${page_no}";
    private static final String VARIABLE_PREFIX = "${";
    private static final String VARIABLE_SUFFIX = "}";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition";
    private static final String FILENAME_PARAM = "filename";
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int DEFAULT_TIMEOUT_MS = 300000; // 5分钟

    private static CloseableHttpClient httpClient;
    private static RequestConfig requestConfig;

    private FileDownloadStepMeta meta;
    private FileDownloadStepData data;

    public FileDownloadStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                            int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }

        this.meta = (FileDownloadStepMeta) smi;
        this.data = (FileDownloadStepData) sdi;

        initializeHttpClient();
        initializeRequestConfig();

        logDebug("File Download Step initialized successfully");
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
                .setConnectTimeout(DEFAULT_TIMEOUT_MS)
                .setConnectionRequestTimeout(DEFAULT_TIMEOUT_MS)
                .setSocketTimeout(DEFAULT_TIMEOUT_MS)
                .build();
    }

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

        executeDownload(currentRow);
        putRow(data.outputRowMeta, currentRow);
        incrementLinesOutput();

        return true;
    }

    private boolean isFirstRow() {
        return first;
    }

    /**
     * Initialize the first row of data.
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

    private boolean shouldStopProcessing(Object[] currentRow) {
        if (currentRow == null) {
            setInputRowMeta(new RowMeta());
            return true;
        }
        return false;
    }

    /**
     * Execute the download operation for the current row.
     */
    private void executeDownload(Object[] currentRow) throws KettleException {
        String url = buildRequestUrl(currentRow);

        if (WayType.GET.getDescription().equals(meta.getRequestMethod())) {
            executeGetDownload(currentRow, url);
        } else {
            executePostDownload(currentRow, url);
        }
    }

    /**
     * Build the request URL for the download operation.
     */
    private String buildRequestUrl(Object[] currentRow) {
        return getUrlValue(meta.getUrl(), currentRow, 1L);
    }

    /**
     * Execute the GET download operation for the current row.
     */
    private void executeGetDownload(Object[] currentRow, String url) throws KettleException {
        executeWithRetry(() -> {
            String finalUrl = url;
            if (meta.isUseXForm()) {
                finalUrl = appendXFormsToUrl(url, currentRow);
            }

            HttpGet httpGet = new HttpGet(finalUrl);
            httpGet.setConfig(requestConfig);
            addHeaders(currentRow, httpGet);

            return httpGet;
        }, currentRow);
    }

    private void executePostDownload(Object[] currentRow, String url) throws KettleException {
        executeWithRetry(() -> {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            addHeaders(currentRow, httpPost);

            if (meta.isUseXForm()) {
                addPostXForms(httpPost, currentRow);
            }
            if (meta.isUseRaw()) {
                addPostRawBody(httpPost, currentRow);
            }

            return httpPost;
        }, currentRow);
    }

    @FunctionalInterface
    private interface RequestBuilder {
        HttpUriRequest build() throws Exception;
    }

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

                validateResponse(response, request.getURI().toString());
                downloadFile(response, currentRow);
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

        throw new KettleException("File download failed after " + maxRetries + " retries", lastException);
    }

    private void validateResponse(HttpResponse response, String url) throws KettleException {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= HTTP_BAD_REQUEST) {
            throw new KettleException(String.format(
                    "HTTP request failed: URL=%s, StatusCode=%d", url, statusCode));
        }
    }

    private void downloadFile(HttpResponse response, Object[] currentRow) throws IOException {
        String fileName = determineFileName(response, currentRow);
        Path targetPath = buildTargetPath(fileName);

        ensureParentDirectoryExists(targetPath);
        writeFileContent(response, targetPath);

        logBasic("File downloaded successfully: " + targetPath);
    }

    private String determineFileName(HttpResponse response, Object[] currentRow) {
        // 优先使用配置的文件名
        if (StringUtils.isNotBlank(meta.getFileName())) {
            return getValue(meta.getFileName(), currentRow, 1L).toString();
        }

        // 从响应头获取文件名
        String fileName = extractFileNameFromResponse(response);
        if (StringUtils.isNotBlank(fileName)) {
            return fileName;
        }

        // 使用步骤名作为默认文件名
        return getStepname();
    }

    private String extractFileNameFromResponse(HttpResponse response) {
        Header contentHeader = response.getFirstHeader(CONTENT_DISPOSITION_HEADER);
        if (contentHeader == null) {
            return null;
        }

        HeaderElement[] elements = contentHeader.getElements();
        if (elements.length != 1) {
            return null;
        }

        NameValuePair param = elements[0].getParameterByName(FILENAME_PARAM);
        if (param == null) {
            return null;
        }

        try {
            return URLDecoder.decode(param.getValue(), DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Failed to decode filename", e);
            return param.getValue();
        }
    }

    private Path buildTargetPath(String fileName) {
        String filePath = environmentSubstitute(meta.getFilePath());
        return Paths.get(filePath, fileName);
    }

    private void ensureParentDirectoryExists(Path filePath) throws IOException {
        Path parentDir = filePath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
            logDebug("Created directory: " + parentDir);
        }
    }

    private void writeFileContent(HttpResponse response, Path targetPath) throws IOException {
        try (InputStream inputStream = response.getEntity().getContent();
             FileOutputStream fileOutputStream = new FileOutputStream(targetPath.toFile())) {

            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int bytesRead;
            long totalBytes = 0;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }

            logDebug("Downloaded " + totalBytes + " bytes to " + targetPath);
        }
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

    private void logRetryAttempt(int attemptNumber, HttpUriRequest request, long delayMs) {
        String url = request != null ? request.getURI().toString() : "unknown";
        long delaySeconds = TimeUnit.MILLISECONDS.toSeconds(delayMs);
        logBasic(String.format("Download failed: %s, retry %d after %d seconds",
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
        logError("File download failed after all retries", e);
        setErrors(1L);
        stopAll();
        throw new KettleException(e);
    }

    private void addPostXForms(HttpPost httpPost, Object[] currentRow)
            throws UnsupportedEncodingException {
        List<Param> xforms = meta.getxFormParams();
        if (xforms == null || xforms.isEmpty()) {
            return;
        }

        List<NameValuePair> params = Lists.newArrayList();
        for (Param param : xforms) {
            String value = environmentSubstitute(
                    String.valueOf(getValue(param.getValue(), currentRow, 1L)));
            params.add(new BasicNameValuePair(param.getName(), value));
        }

        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, DEFAULT_ENCODING);
        httpPost.setEntity(entity);
    }

    private void addPostRawBody(HttpPost httpPost, Object[] currentRow) {
        String rawBody = meta.getRaw();
        Object processedValue = getValue(rawBody, currentRow, 1L);
        String content = environmentSubstitute(String.valueOf(processedValue));

        StringEntity entity = new StringEntity(content, DEFAULT_ENCODING);
        entity.setContentType(meta.getRequestType());
        entity.setContentEncoding(new BasicHeader("Content-Encoding", DEFAULT_ENCODING));
        httpPost.setEntity(entity);
    }

    private void addHeaders(Object[] currentRow, HttpUriRequest request) {
        List<Param> headers = meta.getParams();
        boolean hasContentType = false;

        if (headers != null && !headers.isEmpty()) {
            for (Param header : headers) {
                String value = environmentSubstitute(
                        String.valueOf(getValue(header.getValue(), currentRow, 1L)));
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

    private String appendXFormsToUrl(String baseUrl, Object[] currentRow)
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
                    String.valueOf(getValue(xform.getValue(), currentRow, 1L)));
            urlBuilder.append(value);

            separator = "&";
        }

        return urlBuilder.toString();
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
        this.meta = (FileDownloadStepMeta) smi;
        this.data = (FileDownloadStepData) sdi;

        super.dispose(smi, sdi);
        logDebug("File Download Step disposed");
    }
}