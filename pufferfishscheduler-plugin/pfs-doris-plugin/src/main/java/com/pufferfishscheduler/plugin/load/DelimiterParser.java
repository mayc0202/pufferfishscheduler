package com.pufferfishscheduler.plugin.load;

import com.google.common.base.Strings;

import java.io.StringWriter;

/**
 * @Author: yc
 * @CreateTime: 2024-11-07
 * @Description: 分隔符解析器
 * @Version: 1.0
 */
public class DelimiterParser {
    private static final String HEX_STRING = "0123456789ABCDEF";

    public static String parse(String sp, String dSp) throws RuntimeException {
        if (Strings.isNullOrEmpty(sp)) {
            return dSp;
        }
        if (!sp.toUpperCase().startsWith("\\X")) {
            return sp;
        }
        String hexStr = sp.substring(2);

        // 校验
        if (hexStr.isEmpty()) {
            throw new RuntimeException("解析分隔符失败: `Hex-str为空`");
        }
        if (hexStr.length() % 2 != 0) {
            throw new RuntimeException("解析分隔符失败：`Hex-str字符串长度错误`");
        }
        for (char hexChar : hexStr.toUpperCase().toCharArray()) {
            if (HEX_STRING.indexOf(hexChar) == -1) {
                throw new RuntimeException("解析分隔符失败：`Hex-str格式错误`");
            }
        }

        // 转换为分隔符
        StringWriter writer = new StringWriter();
        for (byte b : hexStrToBytes(hexStr)) {
            writer.append((char) b);
        }
        return writer.toString();
    }

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }

    public static String convertSeparator(String separator) {
        if (null == separator || "".equals(separator)) {
            return separator;
        }
        return separator.replace("\\t","\t")
                .replace("\\n","\n")
                .replace("\\r","\r")
                .replace("\\b","\b")
                .replace("\\f","\f");
    }
}