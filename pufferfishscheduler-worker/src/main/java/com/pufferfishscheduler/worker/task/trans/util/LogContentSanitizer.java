package com.pufferfishscheduler.worker.task.trans.util;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 落库 / 对外展示前的日志与参数脱敏（降低连接串、口令、Token 泄露风险）。
 */
public final class LogContentSanitizer {

    private static final Pattern JDBC_USER_PASS =
            Pattern.compile("(jdbc:[^:\\s]+://)([^:/@\\s]+):([^@\\s]+)@",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern KEY_VALUE_SECRET = Pattern.compile(
            "(?i)\\b(password|pwd|passwd|pass|secret|token|authorization|apikey|api_key)\\b\\s*[:=]\\s*([^\\s&;,\"']+)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern JSON_SECRET = Pattern.compile(
            "(?i)\"(password|pwd|passwd|pass|secret|token|apiKey|api_key)\"\\s*:\\s*\"([^\"]*)\"");
    private static final int MAX_LEN = 8000;

    private LogContentSanitizer() {
    }

    /**
     * 对可能写入 DB 的大段文本做脱敏与截断。
     */
    public static String sanitizeForStorage(String raw) {
        if (StringUtils.isBlank(raw)) {
            return raw;
        }
        String s = raw;
        s = maskJdbcCredentials(s);
        s = maskKeyValueSecrets(s);
        s = maskJsonSecrets(s);
        if (s.length() > MAX_LEN) {
            return s.substring(0, MAX_LEN) + "…[truncated]";
        }
        return s;
    }

    private static String maskJdbcCredentials(String s) {
        Matcher m = JDBC_USER_PASS.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, Matcher.quoteReplacement(m.group(1) + m.group(2) + ":***@"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String maskKeyValueSecrets(String s) {
        Matcher m = KEY_VALUE_SECRET.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, Matcher.quoteReplacement(m.group(1) + ":***"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String maskJsonSecrets(String s) {
        Matcher m = JSON_SECRET.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb,
                    Matcher.quoteReplacement("\"" + m.group(1) + "\":\"***\""));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
