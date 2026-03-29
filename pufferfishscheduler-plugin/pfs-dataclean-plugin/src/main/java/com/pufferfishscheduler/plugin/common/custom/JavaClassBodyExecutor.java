package com.pufferfishscheduler.plugin.common.custom;

import com.pufferfishscheduler.plugin.DataCleanStep;
import com.pufferfishscheduler.plugin.common.custom.api.IJavaCheck;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;

import java.io.Serializable;


public class JavaClassBodyExecutor implements Serializable {

    private static final long serialVersionUID = -6673029679663242922L;

    transient IClassBodyEvaluator cbe;
    IJavaCheck o;

    public void init(String snippet) throws Exception {
        cbe = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator();
        cbe.setImplementedInterfaces(new Class[]{IJavaCheck.class});
        cbe.setParentClassLoader(DataCleanStep.class.getClassLoader());
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
        classBuffer.append("import com.pufferfishscheduler.plugin.common.custom.Param;\n" +
                "\n");
        classBuffer.append("import com.pufferfishscheduler.plugin.common.custom.api.IJavaCheck;\n" +
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