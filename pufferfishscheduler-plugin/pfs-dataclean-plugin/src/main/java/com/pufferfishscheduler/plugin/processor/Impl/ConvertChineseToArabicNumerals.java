package com.pufferfishscheduler.plugin.processor.Impl;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleException;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: yc
 * @CreateTime: 2025-05-26
 * @Description: 中文转阿拉伯数字
 * @Version: 1.0
 */
public class ConvertChineseToArabicNumerals implements RuleProcessor {

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
    public Object convert(Object value) throws KettleException, SQLException, UnsupportedEncodingException {
        // 转成阿拉伯数字
        String v = String.valueOf(value);
        if (null != v && !"".equals(v) && !Constants.NULL.equals(v)) {
            return transFormation(v);
        }
        return value;
    }

    /**
     * 中文汉字转阿拉伯数字-转换
     * @param chinese
     * @return
     */
    private Double transFormation(String chinese) {
        // 针对"拾"字开头的添加一个"壹"
        if (chinese.startsWith("拾") || chinese.startsWith("十")) chinese = "壹" + chinese;
        List<Double> tenThousand = new ArrayList<>();
        double hundredMillion = 0.0;
        double currentNumber = 0.0;
        Double multiple = 0.0;
        Double result = 0.0;
        boolean isNewPart = false;
        char[] chars = chinese.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            String charStr = String.valueOf(chars[i]);
            if (cacheNumber.containsKey(charStr)) {
                if (isNewPart) {
                    tenThousand.add(currentNumber);
                    isNewPart = false;
                    currentNumber = 0.0; // 重置当前数字
                }
                currentNumber += cacheNumber.get(charStr);
            } else if (cacheUnit2.containsKey(charStr)) {
                multiple = cacheUnit2.get(charStr);
                if (charStr.equals("亿")) {
                    // 特殊处理
                    Double sum = 0.0;
                    for (Double part : tenThousand) {
                        sum += part;
                    }
                    // 清空集合
                    tenThousand.clear();
                    currentNumber = (sum + currentNumber) * 10000 * 10000;
                    hundredMillion = currentNumber;
                    currentNumber = 0.0;
                } else if (charStr.equals("万")) {
                    // 特殊处理
                    Double sum = 0.0;
                    for (Double part : tenThousand) {
                        sum += part;
                    }
                    // 清空集合
                    tenThousand.clear();
                    currentNumber = (sum + currentNumber) * 10000;
                } else {
                    currentNumber *= multiple;
                }
                isNewPart = true;
                // 添加最后一位
                if (i == chars.length - 1) {
                    tenThousand.add(currentNumber);
                }
            }
        }

        // 累加结果
        for (Double part : tenThousand) {
            result += part;
        }
        return result + hundredMillion;
    }
}