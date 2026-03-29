package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 阿拉伯数字转中文汉字
 */
public class ConvertArabicNumeralsToChinese implements RuleProcessor {

    /* 缓存0-9 */
    private static final Map<Integer,String> cacheChinese = new HashMap<>();
    /* 缓存单位 */
    private static final Map<Integer,String> cacheUnit= new HashMap<>();
    /* 缓存零-玖 */
    private static final Map<String,Double> cacheNumber = new HashMap<>();
    /* 缓存单位2 */
    private static final Map<String,Double> cacheUnit2 = new HashMap<>();

    static {
        cacheChinese.put(0,"零");
        cacheChinese.put(1,"壹");
        cacheChinese.put(2,"贰");
        cacheChinese.put(3,"叁");
        cacheChinese.put(4,"肆");
        cacheChinese.put(5,"伍");
        cacheChinese.put(6,"陆");
        cacheChinese.put(7,"柒");
        cacheChinese.put(8,"捌");
        cacheChinese.put(9,"玖");

        cacheUnit.put(-2,"哩");
        cacheUnit.put(-1,"分");
        cacheUnit.put(0,"角");
        cacheUnit.put(1,"元");
        cacheUnit.put(2,"拾");
        cacheUnit.put(3,"百");
        cacheUnit.put(4,"仟");
        cacheUnit.put(5,"万");
        cacheUnit.put(6,"拾");
        cacheUnit.put(7,"百");
        cacheUnit.put(8,"仟");
        cacheUnit.put(9,"亿");
        cacheUnit.put(10,"拾");

        cacheNumber.put("零",0.0);
        cacheNumber.put("壹",1.0);
        cacheNumber.put("一",1.0);
        cacheNumber.put("贰",2.0);
        cacheNumber.put("二",2.0);
        cacheNumber.put("叁",3.0);
        cacheNumber.put("三",3.0);
        cacheNumber.put("肆",4.0);
        cacheNumber.put("四",4.0);
        cacheNumber.put("伍",5.0);
        cacheNumber.put("五",5.0);
        cacheNumber.put("陆",6.0);
        cacheNumber.put("六",6.0);
        cacheNumber.put("柒",7.0);
        cacheNumber.put("七",7.0);
        cacheNumber.put("捌",8.0);
        cacheNumber.put("八",8.0);
        cacheNumber.put("玖",9.0);
        cacheNumber.put("九",9.0);

        cacheUnit2.put("哩",0.001);
        cacheUnit2.put("分",0.01);
        cacheUnit2.put("角",0.1);
        cacheUnit2.put("元",1.0);
        cacheUnit2.put("圆",1.0);
        cacheUnit2.put("拾",10.0);
        cacheUnit2.put("十",10.0);
        cacheUnit2.put("佰",100.0);
        cacheUnit2.put("百",100.0);
        cacheUnit2.put("仟",1000.0);
        cacheUnit2.put("千",1000.0);
        cacheUnit2.put("万",10000.0);
        cacheUnit2.put("亿",100000000.0);
    }

    @Override
    public void init(JSONObject metadata) throws KettleStepException {

    }

    @Override
    public Object convert(Object value) throws KettleStepException {
        if (value != null) {
            // 处理 Integer 或 Double 类型
            double numericValue;
            if (value instanceof Integer) {
                numericValue = ((Integer) value).doubleValue();
            } else if (value instanceof Long) {
                numericValue = ((Long) value).doubleValue();
            } else if (value instanceof Double) {
                numericValue = (Double) value;
            } else {
                return value;
            }
            return transFormation(numericValue);
        }
        return null;
    }

    /**
     * 阿拉伯数字转中文汉字-转换（修改后：用BigDecimal精确拆分数字，避免精度丢失）
     * @param number 待转换的数字
     * @return 字符列表 + 单位计算所需的num值
     */
    private StringBuffer transFormation(Double number) {
        List<String> charList = new ArrayList<>();

        // 1. 用BigDecimal处理浮点数，避免精度丢失
        BigDecimal bd = new BigDecimal(number.toString());
        // 保留2位小数（金额通常到“分”，需四舍五入）
        bd = bd.setScale(2, RoundingMode.HALF_UP);

        // 2. 拆分整数部分和小数部分
        String numStr = bd.toString();
        String[] parts = numStr.split("\\.");
        String integerPart = parts[0];        // 整数部分（如"5800"）
        String decimalPart = parts.length > 1 ? parts[1] : "00"; // 小数部分（默认补0）

        // 3. 确保小数部分是2位（不足补0，超过截断）
        if (decimalPart.length() < 2) {
            int needZeroCount = 2 - decimalPart.length(); // 需要补的零的个数
            StringBuilder zeroBuilder = new StringBuilder();
            for (int i = 0; i < needZeroCount; i++) {
                zeroBuilder.append("0"); // 循环拼接指定数量的"0"
            }
            decimalPart = decimalPart + zeroBuilder.toString(); // 补零后拼接
        }

        // 4. 合并整数+小数的每一位数字（如5800.56 → "580056"）
        String fullDigits = integerPart + decimalPart;
        char[] digits = fullDigits.toCharArray();

        // 5. 逐位转换为中文数字（如'5'→"伍"）
        for (char c : digits) {
            int digit = Character.getNumericValue(c);
            charList.add(cacheChinese.get(digit));
        }

        // 6. 计算单位匹配的初始num（小数点在整数部分末尾，如5800 → 长度4，num=4+1=5）
        int num = integerPart.length() + 1;

        return splicing(charList, num);
    }

    /**
     * 阿拉伯数字转中文汉字-处理单位
     * @param list
     * @param num
     * @return
     */
    private StringBuffer splicing(List<String> list, int num) {
        StringBuffer stringBuffer = new StringBuffer();
        for (String s : list) {
            num--;
            if (num < 0) {
                stringBuffer.append(s).append(cacheUnit.get(num));
            } else {
                stringBuffer.append(s).append(cacheUnit.get(num));
            }
        }
        return stringBuffer;
    }

}
