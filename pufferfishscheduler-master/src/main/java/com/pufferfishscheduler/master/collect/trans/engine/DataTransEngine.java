package com.pufferfishscheduler.master.collect.trans.engine;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.SwingGC;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.MetricsRegistry;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainter;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.TransComponent;
import com.pufferfishscheduler.dao.mapper.TransComponentMapper;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.master.collect.trans.engine.entity.JobExecutingData;
import com.pufferfishscheduler.master.collect.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;
import com.pufferfishscheduler.master.collect.trans.engine.exception.DataTransformationEngineException;
import com.pufferfishscheduler.master.collect.trans.engine.listener.KettleStepRowListener;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import lombok.extern.slf4j.Slf4j;

/**
 * 数据转换引擎
 * 
 * @author Mayc
 * @since 2026-03-04 17:44
 */
@Slf4j
public class DataTransEngine {
    private static final Pattern MISSING_STEP_ICON_PATTERN = Pattern.compile(
            "Unable to find file: (.+?) for plugin: ([^/\\{]+)");
    /**
     * 默认批次号
     */
    public static final String DEFAULT_BATCHID = "instanceId";// 默认批次号
    /**
     * 默认批次号名称
     */
    public static final String DEFAULT_BATCHID_NAME = "执行转换批次号。【系统内置参数】";
    /**
     * 默认插件目录（Kettle KETTLE_PLUGIN_BASE_FOLDERS 的基础目录）
     * StepPluginType 内部调用 populateFolders("steps")，会在此基础上追加 "/steps" 后扫描 plugin.xml。
     * 因此这里填 "plugins"，Kettle 实际扫描的是 classpath:plugins/steps/，
     * antrun 也正是把插件解压到 plugins/steps/pfs-dataclean-plugin-1.0.0/ 下。
     */
    public static final String DEFAULT_PLUGIN_DIR = "plugins";
    /**
     * 限流次数参数名
     */
    public static final String RATE_LIMIT_COUNT = "RATE_LIMIT_COUNT";
    /**
     * DTDesigner插件目录
     */
    private String dtDesignerPluginsDir;
    /**
     * 转换组件Mapper（用于在缺失图标时从DB回填）
     */
    private TransComponentMapper transComponentMapper;
    /**
     * 转换Map
     */
    private static final ConcurrentHashMap<Integer, TransWrapper> TRANS_MAP = new ConcurrentHashMap<>();

    /**
     * 初始化引擎
     */
    public void init() {
        // kettle初始化运行环境，包括加载插件
        try {
            // 设置插件目录
            String pluginsDir = getPluginsDir();
            log.info(String.format("Load plugins from '%s'", pluginsDir));
            System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", pluginsDir);
            System.setProperty("KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "Y");
            KettleEnvironment.init(false);

            // 加载插件JAR文件
            loadPluginJars(pluginsDir);

            log.info("Kettle environment initialized successfully.");
        } catch (Exception e) {
            throw new DataTransformationEngineException("初始化引擎失败！", e);
        }
    }

    /**
     * 加载插件JAR文件
     * 
     * @param pluginsDir 插件目录
     * @throws Exception 加载异常
     */
    private void loadPluginJars(String pluginsDir) throws Exception {
        File pluginFile = new File(pluginsDir);
        if (!pluginFile.exists() || !pluginFile.isDirectory()) {
            log.warn("Plugin directory does not exist: {}", pluginsDir);
            return;
        }

        // 加载JAR文件
        File[] jars = pluginFile.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jars != null && jars.length > 0) {
            log.info("Found {} plugin JARs in {}", jars.length, pluginsDir);
            for (File jar : jars) {
                try {
                    // 注意：这里只是记录发现的JAR文件，实际的类加载会在使用时发生
                    log.debug("Found plugin JAR: {}", jar.getName());
                } catch (Exception e) {
                    log.warn("Failed to process plugin JAR: {}", jar.getName(), e);
                }
            }
        } else {
            log.info("No plugin JARs found in {}", pluginsDir);
        }
    }

    /**
     * 获取插件目录
     *
     * @return 插件目录路径
     */
    private String getPluginsDir() {
        if (StringUtils.isNotEmpty(dtDesignerPluginsDir)) {
            return dtDesignerPluginsDir;
        }
        // 关键：不要复用外部注入的 KETTLE_PLUGIN_BASE_FOLDERS，否则可能指向一个不包含自研插件的目录，
        // 最终运行期表现为 plugin missing。这里统一走应用自带的 classpath plugins 目录。
        return getDefaultPluginsDir();
    }

    /**
     * 获取默认插件目录
     *
     * @return 默认插件目录路径
     */
    private String getDefaultPluginsDir() {
        File classpathPluginsDir = null;
        try {
            String parentPath = URLDecoder.decode(this.getClass().getClassLoader().getResource(".").getPath(), "utf-8");
            classpathPluginsDir = new File(parentPath, DEFAULT_PLUGIN_DIR);
        } catch (UnsupportedEncodingException e) {
            throw new DataTransformationEngineException(String.format("获取默认插件目录失败！[错误:%s]", e.getMessage()));
        }

        if (classpathPluginsDir != null && classpathPluginsDir.exists() && classpathPluginsDir.isDirectory()) {
            return classpathPluginsDir.getAbsolutePath();
        }

        // 优先使用 KETTLE_HOME，避免加载到不完整的 classpath 插件目录导致步骤图标缺失
        String kettleHome = System.getProperty("KETTLE_HOME");
        if (StringUtils.isNotBlank(kettleHome)) {
            File kettleHomePluginsDir = new File(kettleHome, DEFAULT_PLUGIN_DIR);
            if (kettleHomePluginsDir.exists() && kettleHomePluginsDir.isDirectory()) {
                return kettleHomePluginsDir.getAbsolutePath();
            }
        }

        // 回退到工作目录 plugins
        File userDirPluginsDir = new File(System.getProperty("user.dir"), DEFAULT_PLUGIN_DIR);
        if (userDirPluginsDir.exists() && userDirPluginsDir.isDirectory()) {
            return userDirPluginsDir.getAbsolutePath();
        }

        // 最后兜底返回 classpath 位置（可能不存在），由后续初始化日志提示
        return classpathPluginsDir == null ? DEFAULT_PLUGIN_DIR : classpathPluginsDir.getAbsolutePath();
    }

    /**
     * 获取转换步骤
     *
     * @param id 转换ID
     * @return 步骤名称数组
     */
    public String[] getSteps(Integer id) {
        TransWrapper transWrapper = TRANS_MAP.get(id);

        if (transWrapper == null) {
            return new String[0];
        }

        TransMeta transMeta = transWrapper.getTransMeta();
        return transMeta.getStepNames();
    }

    /**
     * 重置日志
     */
    public void resetLog() {
        KettleLogStore.getAppender().clear();
        KettleLogStore.getInstance().reset();
        KettleLogStore.init();
        LoggingRegistry.getInstance().reset();
        MetricsRegistry.getInstance().reset();
        TRANS_MAP.clear();
    }

    /**
     * 移除转换
     *
     * @param id 转换ID
     */
    public void removeTrans(Integer id) {
        TransWrapper transWrapper = TRANS_MAP.get(id);

        if (transWrapper != null) {
            // 等待部分步骤处理完成
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.warn("Thread interrupted while waiting for steps to complete", e);
                Thread.currentThread().interrupt();
            }

            // 清理日志和执行信息
            cleanupTransResources(transWrapper);

            // 从映射中移除
            TRANS_MAP.remove(id);
        }
    }

    /**
     * 清理转换资源
     * 
     * @param transWrapper 转换包装器
     */
    private void cleanupTransResources(TransWrapper transWrapper) {
        // 清理日志
        KettleLogStore.discardLines(transWrapper.getLogChannelId(), true);

        // 清理执行信息
        String instanceId = transWrapper.getInstanceId();
        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo != null) {
            jobExecutingInfo.getJobEntryExcutinginfos().clear();
            JobExecutingData.remove(instanceId);
        }
    }

    /**
     * 检查转换是否存在
     *
     * @param id 转换ID
     * @return 是否存在
     */
    public boolean checkTransStatus(Integer id) {
        return TRANS_MAP.get(id) != null;
    }

    /**
     * 停止转换
     *
     * @param trans 转换包装器
     */
    public void stop(TransWrapper trans) {
        if (trans != null) {
            trans.stopAll();
        }
    }

    /**
     * 停止转换
     *
     * @param id 转换ID
     */
    public void stopTrans(Integer id) {
        TransWrapper transWrapper = TRANS_MAP.get(id);
        if (transWrapper != null) {
            transWrapper.stopAll();
        }
    }

    /**
     * 执行转换
     *
     * @param transFlow     转换流程
     * @param params        转换参数
     * @param transListener 转换监听器。转换执行开始和结束后，可通过此监听器做相应业务操作
     * @param stepListener  转换步骤监听器
     * @param rowListener   行监听器
     * @return 转换包装器
     * @throws IOException IO异常
     */
    public TransWrapper executeTrans(TransFlow transFlow,
            List<TransParam> params,
            TransListener transListener,
            StepListener stepListener,
            KettleStepRowListener rowListener) throws IOException {

        if (transFlow == null) {
            throw new DataTransformationEngineException("转换流程不能为空");
        }

        String stage = transFlow.getStage();
        Integer id = transFlow.getId();

        TransWrapper trans = null;
        try {
            // 加载转换元数据
            TransMeta transMeta = loadTransMeta(stage, id);

            // 创建转换
            trans = createTransWrapper(transMeta);

            // 设置转换执行参数
            params = setParamsOfTrans(trans, params);
            trans.setParams(params);

            // 添加参数定义到转换元数据
            addParametersToTransMeta(transMeta, params);

            // 设置监听器
            setupListeners(trans, transListener, stepListener, rowListener);

            // 启动转换
            startTrans(trans, id);

            return trans;
        } catch (Exception e) {
            // 关键：保留原始 cause，否则只能看到“无法初始化至少一个步骤”而无法定位具体步骤/原因
            String rawMessage = e.getMessage();
            if (StringUtils.isBlank(rawMessage)) {
                rawMessage = e.toString();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(rawMessage);
            sb.append("<br/><br/>[exception]=").append(e.getClass().getName());
            if (e.getCause() != null) {
                sb.append("<br/>[cause]=").append(e.getCause().getClass().getName())
                  .append(" : ").append(e.getCause().getMessage());
            }

            // 尝试附带转换名称与日志通道，方便直接去 Kettle 日志里定位
            if (trans != null) {
                sb.append("<br/>[trans]=").append(trans.getName());
                sb.append("<br/>[logChannelId]=").append(trans.getLogChannelId());
            }

            String htmlMessage = sb.toString().replaceAll("(\r\n|\r|\n|\n\r)", "<br/>");
            log.error("执行转换失败：stage={}, id={}", stage, id, e);
            throw new DataTransformationEngineException(htmlMessage, e);
        }
    }

    /**
     * 加载转换元数据
     * 
     * @param stage 阶段
     * @param id    转换ID
     * @return 转换元数据
     * @throws Exception 加载异常
     */
    private TransMeta loadTransMeta(String stage, Integer id) throws Exception {
        DataFlowRepository repository = DataFlowRepository.getRepository();
        TransFlowConfig transFlowConfig = repository.getTrans(stage + "_" + Constants.TRANS, id.toString());
        if (transFlowConfig == null) {
            throw new DataTransformationEngineException(
                    "Trans flow config not found for stage: " + stage + ", id: " + id);
        }
        String config = transFlowConfig.getFlowContent();
        if (StringUtils.isBlank(config)) {
            throw new DataTransformationEngineException(
                    "Trans flow config content is empty for stage: " + stage + ", id: " + id);
        }
        return DataFlowRepository.xml2TransMeta(config);
    }

    /**
     * 创建转换包装器
     * 
     * @param transMeta 转换元数据
     * @return 转换包装器
     * @throws KettleException Kettle异常
     */
    private TransWrapper createTransWrapper(TransMeta transMeta) throws KettleException {
        TransWrapper trans = new TransWrapper(transMeta);
        trans.getTransMeta().setInternalKettleVariables();
        trans.copyParametersFrom(transMeta);

        // 设置转换状态
        trans.setMonitored(true);
        trans.setInitializing(true);
        trans.setPreparing(true);
        trans.setRunning(true);
        trans.setSafeModeEnabled(true);

        return trans;
    }

    /**
     * 设置转换执行参数
     *
     * @param trans  转换包装器
     * @param params 参数列表
     * @return 更新后的参数列表
     */
    private List<TransParam> setParamsOfTrans(TransWrapper trans, List<TransParam> params) {
        try {
            if (params == null) {
                params = new ArrayList<>();
            }

            // 添加默认批次号参数
            TransParam batchIdParam = new TransParam(
                    DEFAULT_BATCHID,
                    trans.getInstanceId(),
                    "",
                    DEFAULT_BATCHID_NAME);
            params.add(batchIdParam);

            // 添加参数定义并设置变量
            for (TransParam param : params) {
                String value = StringUtils.isEmpty(param.getValue())
                        ? param.getDefaultValue()
                        : param.getValue();

                trans.addParameterDefinition(param.getName(), value, "");

                // 特殊处理限流参数
                if (RATE_LIMIT_COUNT.equals(param.getName())) {
                    trans.setVariable(param.getName(), value);
                }
            }

            trans.activateParameters();
            return params;
        } catch (KettleException e) {
            log.error("设置交换执行参数失败！", e);
            throw new DataTransformationEngineException("设置交换执行参数失败！", e);
        }
    }

    /**
     * 添加参数定义到转换元数据
     * 
     * @param transMeta 转换元数据
     * @param params    参数列表
     * @throws KettleException
     */
    private void addParametersToTransMeta(TransMeta transMeta, List<TransParam> params) throws KettleException {
        for (TransParam param : params) {
            transMeta.addParameterDefinition(
                    param.getName(),
                    param.getValue(),
                    param.getDescription());
        }
        transMeta.activateParameters();
    }

    /**
     * 设置监听器
     * 
     * @param trans         转换包装器
     * @param transListener 转换监听器
     * @param stepListener  步骤监听器
     * @param rowListener   行监听器
     * @throws KettleException Kettle异常
     */
    private void setupListeners(TransWrapper trans, TransListener transListener,
            StepListener stepListener, KettleStepRowListener rowListener) throws KettleException {
        // 添加转换监听器
        if (transListener != null) {
            trans.addTransListener(transListener);
        }

        // 准备执行
        trans.prepareExecution(null);

        // 为每个步骤添加监听器
        List<StepMetaDataCombi> stepList = trans.getSteps();
        for (StepMetaDataCombi stepMetaDataCombi : stepList) {
            // 添加行监听器
            if (rowListener != null) {
                KettleStepRowListener clonedRowListener = rowListener.clone();
                clonedRowListener.setStep(stepMetaDataCombi);
                stepMetaDataCombi.step.addRowListener(clonedRowListener);
            }

            // 添加步骤监听器
            if (stepListener != null) {
                stepMetaDataCombi.step.addStepListener(stepListener);
            }
        }
    }

    /**
     * 启动转换
     * 
     * @param trans 转换包装器
     * @param id    转换ID
     * @throws KettleException
     */
    private void startTrans(TransWrapper trans, Integer id) throws KettleException {
        TRANS_MAP.put(id, trans);
        trans.startThreads();
    }

    /**
     * 执行类型枚举
     */
    public enum ExecutingType {
        manual, // 手工
        timing // 定时
    }

    /**
     * 资源类型枚举
     */
    public enum ResourceType {
        JOB,
        TRANS
    }

    /**
     * 设置插件目录
     * 
     * @param dtDesignerPluginsDir 插件目录
     */
    public void setDtDesignerPluginsDir(String dtDesignerPluginsDir) {
        this.dtDesignerPluginsDir = dtDesignerPluginsDir;
    }

    /**
     * 获取插件目录
     * 
     * @return 插件目录
     */
    public String getDtDesignerPluginsDir() {
        return dtDesignerPluginsDir;
    }

    public void setTransComponentMapper(TransComponentMapper transComponentMapper) {
        this.transComponentMapper = transComponentMapper;
    }

    /**
     * 获取转换的base64图片
     * html中使用时，需要在<img src="data:image/png;base64,{{put your base64 pic code
     * here}}"></img>
     *
     * @param id   转换ID
     * @param type 转换类型
     * @return base64编码的图片
     */
    public String getBase64TransImage(Integer id, String type) {
        // 参数验证
        if (id == null) {
            throw new DataTransformationEngineException("转换ID不能为空");
        }
        if (StringUtils.isEmpty(type)) {
            throw new DataTransformationEngineException("转换类型不能为空");
        }

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            // 获取转换元数据
            TransMeta transMeta = loadTransMetaForImage(type, id);

            // 生成转换图片（若缺失步骤图标，则尝试从trans_component回填后重试一次）
            BufferedImage bufferedImage;
            try {
                bufferedImage = generateTransformationImage(transMeta);
            } catch (Exception ex) {
                if (hydrateMissingStepIcon(ex)) {
                    bufferedImage = generateTransformationImage(transMeta);
                } else {
                    throw ex;
                }
            }

            // 编码为base64
            Base64.Encoder encoder = Base64.getEncoder();
            ImageIO.write(bufferedImage, "png", bytes);
            byte[] pic = bytes.toByteArray();
            return encoder.encodeToString(pic);

        } catch (Exception e) {
            log.error("获取转换截图失败！ID: {}, Type: {}", id, type, e);
            throw new DataTransformationEngineException("获取转换截图失败！", e);
        }
    }

    /**
     * 加载转换元数据用于生成图片
     * 
     * @param type 转换类型
     * @param id   转换ID
     * @return 转换元数据
     * @throws Exception 加载异常
     */
    private TransMeta loadTransMetaForImage(String type, Integer id) throws Exception {
        DataFlowRepository repository = DataFlowRepository.getRepository();
        TransFlowConfig transFlowConfig = repository.getTrans(type + "_" + Constants.TRANS, id.toString());
        if (transFlowConfig == null) {
            throw new DataTransformationEngineException(
                    "Trans flow config not found for type: " + type + ", id: " + id);
        }
        String config = transFlowConfig.getFlowContent();
        if (StringUtils.isBlank(config)) {
            throw new DataTransformationEngineException(
                    "Trans flow config content is empty for type: " + type + ", id: " + id);
        }
        return DataFlowRepository.xml2TransMeta(config);
    }

    /**
     * 生成转换图片
     * 
     * @param transMeta 转换元数据
     * @return 转换图片
     * @throws Exception 异常
     */
    private static BufferedImage generateTransformationImage(TransMeta transMeta) throws Exception {
        // 参数验证
        if (transMeta == null) {
            throw new IllegalArgumentException("转换元数据不能为空");
        }

        float magnification = 1.0f;
        Point maximum = transMeta.getMaximum();
        maximum.multiply(magnification);

        SwingGC gc = new SwingGC(null, maximum, 32, 0, 0);
        TransPainter transPainter = new TransPainter(
                gc, transMeta, maximum, null, null, null, null, null, new ArrayList<>(),
                new ArrayList<>(), 32, 1, 0, 0, true, "Arial", 10);
        transPainter.setMagnification(magnification);
        transPainter.buildTransformationImage();

        return (BufferedImage) gc.getImage();
    }

    private boolean hydrateMissingStepIcon(Exception ex) {
        if (transComponentMapper == null || ex == null) {
            return false;
        }
        Matcher matcher = MISSING_STEP_ICON_PATTERN.matcher(String.valueOf(ex.getMessage()));
        if (!matcher.find()) {
            return false;
        }
        String relativeIconPath = matcher.group(1).trim();
        String pluginCode = matcher.group(2).trim();
        if (StringUtils.isAnyBlank(relativeIconPath, pluginCode)) {
            return false;
        }

        TransComponent component = findComponentByCode(pluginCode);
        if (component == null || StringUtils.isBlank(component.getIcon())) {
            return false;
        }

        byte[] iconBytes = decodeIconBytes(component.getIcon());
        if (iconBytes.length == 0) {
            return false;
        }

        Path iconFilePath = resolveStepIconFilePath(pluginCode, relativeIconPath);
        if (iconFilePath == null) {
            return false;
        }

        try {
            Path parent = iconFilePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(iconFilePath, iconBytes);
            log.info("Hydrated missing step icon from trans_component. pluginCode={}, iconPath={}", pluginCode, iconFilePath);
            return true;
        } catch (IOException ioEx) {
            log.warn("Failed to write hydrated step icon. pluginCode={}, iconPath={}", pluginCode, iconFilePath, ioEx);
            return false;
        }
    }

    private TransComponent findComponentByCode(String pluginCode) {
        LambdaQueryWrapper<TransComponent> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(TransComponent::getCode, pluginCode).last("limit 1");
        return transComponentMapper.selectOne(wrapper);
    }

    private byte[] decodeIconBytes(String rawIcon) {
        String icon = rawIcon.trim();
        try {
            if (icon.startsWith("data:image/svg+xml;base64,")) {
                return Base64.getDecoder().decode(icon.substring("data:image/svg+xml;base64,".length()));
            }
            if (icon.startsWith("data:image/") && icon.contains(";base64,")) {
                return Base64.getDecoder().decode(icon.substring(icon.indexOf(";base64,") + ";base64,".length()));
            }
            if (icon.startsWith("<svg")) {
                return icon.getBytes(StandardCharsets.UTF_8);
            }
            return Base64.getDecoder().decode(icon);
        } catch (Exception e) {
            log.warn("Failed to decode step icon content from trans_component");
            return new byte[0];
        }
    }

    private Path resolveStepIconFilePath(String pluginCode, String relativeIconPath) {
        String pluginsDir = getPluginsDir();
        if (StringUtils.isBlank(pluginsDir)) {
            return null;
        }
        String[] baseCandidates = pluginsDir.split("[;,]");
        for (String candidate : baseCandidates) {
            if (StringUtils.isBlank(candidate)) {
                continue;
            }
            Path baseDir = Paths.get(candidate.trim());
            if (!Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
                continue;
            }
            Path stepPluginDir = baseDir.resolve("steps").resolve(pluginCode);
            return stepPluginDir.resolve(relativeIconPath.replace("/", File.separator));
        }
        return null;
    }

}
