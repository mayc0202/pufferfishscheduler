package com.pufferfishscheduler.common.codex.java;

import java.io.Serial;
import java.io.Serializable;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;

/**
 * Java自定义规则执行器
 */
public class JavaClassBodyExecutor implements Serializable {

    @Serial
    private static final long serialVersionUID = -6673029679663242922L;

    transient IClassBodyEvaluator cbe;
    IJavaCheck o;

    public void init(String snippet) throws Exception {
        cbe = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator();
        cbe.setImplementedInterfaces(new Class[]{IJavaCheck.class});
        String classBody = constructorClassBody(snippet);
        cbe.cook(classBody);
        o = (IJavaCheck) cbe.getClazz().newInstance();
    }

    /**
     * 自定义规则转换
     *
     * @param param
     * @param value
     * @return
     */
    public Object javaCustomRule(Param param, Object value) {
        return o.convert(param, value);
    }

    /**
     * 导包
     *
     * @param snippet
     * @return
     */
    private String constructorClassBody(String snippet) {

        StringBuffer classBuffer = new StringBuffer();
        // 导包
        classBuffer.append("import com.pufferfishscheduler.common.codex.java.Param;\n" +
                "\n");
        classBuffer.append("import com.pufferfishscheduler.common.codex.java.IJavaCheck;\n" +
                "\n");
        classBuffer.append("import com.alibaba.fastjson2.JSONArray;\n" +
                "\n");
        classBuffer.append("import com.alibaba.fastjson2.JSONObject;\n" +
                "\n");
        classBuffer.append("import java.util.Map;\n" +
                "\n");
        classBuffer.append("import java.util.HashMap;\n" +
                "\n");
        classBuffer.append("import java.util.ArrayList;\n" +
                "\n");
        classBuffer.append("import java.util.List;\n" +
                "\n");
        classBuffer.append("import java.util.*;\n" +
                "\n");
        classBuffer.append("\n\r");
        // 代码块
        classBuffer.append(snippet);
        classBuffer.append("\n\r");
        return classBuffer.toString();
    }

}