package com.pufferfishscheduler.trans.engine;

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
import com.pufferfishscheduler.trans.engine.entity.JobExecutingData;
import com.pufferfishscheduler.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.trans.engine.entity.TransParam;
import com.pufferfishscheduler.trans.engine.exception.DataTransformationEngineException;
import com.pufferfishscheduler.trans.engine.listener.KettleStepRowListener;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * 数据转换引擎
 *
 * 功能模块：
 * 1. Kettle环境初始化
 * 2. 转换生命周期管理（加载、执行、停止、移除）
 * 3. 转换图片生成
 * 4. 插件管理
 * 5. 步骤图标自动回填
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
    public static final String DEFAULT_BATCH_ID = "instanceId";

    /**
     * 默认批次号名称
     */
    public static final String DEFAULT_BATCH_ID_NAME = "执行转换批次号。【系统内置参数】";

    /**
     * 默认插件目录
     * StepPluginType 内部调用 populateFolders("steps")，会在此基础上追加 "/steps" 后扫描 plugin.xml
     */
    public static final String DEFAULT_PLUGIN_DIR = "plugins";
    private static final String CLASSPATH_PLUGINS_DIR = DEFAULT_PLUGIN_DIR;
    private static final String CLASSPATH_STEPS_PREFIX = CLASSPATH_PLUGINS_DIR + "/steps/";

    /**
     * 限流次数参数名
     */
    public static final String RATE_LIMIT_COUNT = "RATE_LIMIT_COUNT";

    private static final int STEP_WAIT_MILLIS = 2000;
    private static final float IMAGE_MAGNIFICATION = 1.0f;
    private static final int IMAGE_ICON_SIZE = 32;
    private static final int IMAGE_ARROW_LENGTH = 32;
    private static final int IMAGE_LINE_WIDTH = 1;
    private static final int IMAGE_GRID_SIZE = 0;
    private static final int IMAGE_START_OFFSET = 0;
    private static final String DEFAULT_FONT_NAME = "Arial";
    private static final int DEFAULT_FONT_SIZE = 10;

    // SVG图片前缀
    private static final String SVG_BASE64_PREFIX = "data:image/svg+xml;base64,";
    private static final String IMAGE_BASE64_PREFIX_PATTERN = "data:image/.*;base64,";

    /**
     * 转换Map
     */
    private static final ConcurrentHashMap<Integer, TransWrapper> TRANS_MAP = new ConcurrentHashMap<>();

    /**
     * DTDesigner插件目录
     */
    private String dtDesignerPluginsDir;

    /**
     * 转换组件Mapper（用于在缺失图标时从DB回填）
     */
    private TransComponentMapper transComponentMapper;

    /**
     * Kettle 是否已完成初始化（与 {@link #init()} 配合，避免异步 Bean 与保存配置并发导致 PluginRegistry 未就绪、步骤 XML 中 &lt;type&gt; 为空）。
     */
    private volatile boolean kettleInitialized;
    private final Object kettleInitMonitor = new Object();

    /**
     * 初始化引擎（幂等；多线程重复调用安全）
     */
    public void init() {
        if (kettleInitialized) {
            return;
        }
        synchronized (kettleInitMonitor) {
            if (kettleInitialized) {
                return;
            }
            try {
                configureKettleEnvironment();
                KettleEnvironment.init(false);
                loadPluginJars(getPluginsDir());
                log.info("Kettle environment initialized successfully.");
                kettleInitialized = true;
            } catch (Exception e) {
                throw new DataTransformationEngineException("初始化引擎失败！", e);
            }
        }
    }

    /**
     * 配置Kettle环境
     */
    private void configureKettleEnvironment() {
        String pluginsDir = getPluginsDir();
        log.info("Load plugins from '{}'", pluginsDir);

        System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", pluginsDir);
        System.setProperty("KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "Y");
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

    // ==================== 转换生命周期管理 ====================

    /**
     * 执行转换
     *
     * @param transFlow     转换流程
     * @param params        转换参数
     * @param transListener 转换监听器
     * @param stepListener  步骤监听器
     * @param rowListener   行监听器
     * @return 转换包装器
     * @throws IOException IO异常
     */
    public TransWrapper executeTrans(TransFlow transFlow, List<TransParam> params,
                                     TransListener transListener, StepListener stepListener,
                                     KettleStepRowListener rowListener) throws IOException {
        return executeTrans(transFlow, null, params, transListener, stepListener, rowListener);
    }

    /**
     * 执行转换（可指定 {@link TRANS_MAP} 的键，例如 Worker 使用 taskId 与流程 id 区分）
     *
     * @param transMapKey 为空时使用 {@link TransFlow#getId()}
     */
    public TransWrapper executeTrans(TransFlow transFlow, Integer transMapKey, List<TransParam> params,
                                     TransListener transListener, StepListener stepListener,
                                     KettleStepRowListener rowListener) throws IOException {

        validateTransFlow(transFlow);

        String stage = transFlow.getStage();
        Integer flowId = transFlow.getId();
        Integer mapKey = transMapKey != null ? transMapKey : flowId;
        TransWrapper trans = null;

        try {
            TransMeta transMeta = loadTransMeta(stage, flowId);
            trans = createTransWrapper(transMeta);

            params = setupTransParameters(trans, params);
            addParametersToTransMeta(transMeta, params);
            setupListeners(trans, transListener, stepListener, rowListener);
            startTrans(trans, mapKey);

            return trans;
        } catch (Exception e) {
            throw createExecutionException(e, trans, stage, flowId);
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
     * 移除转换
     *
     * @param id 转换ID
     */
    public void removeTrans(Integer id) {
        TransWrapper transWrapper = TRANS_MAP.get(id);
        if (transWrapper != null) {
            waitForStepsCompletion();
            cleanupTransResources(transWrapper);
            TRANS_MAP.remove(id);
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
     * 获取转换步骤
     *
     * @param id 转换ID
     * @return 步骤名称数组
     */
    public String[] getSteps(Integer id) {
        TransWrapper transWrapper = TRANS_MAP.get(id);
        return transWrapper == null ? new String[0] : transWrapper.getTransMeta().getStepNames();
    }

    // ==================== 转换图片生成 ====================

    /**
     * 获取转换的base64图片
     *
     * @param id   转换ID
     * @param type 转换类型
     * @return base64编码的图片
     */
    public String getBase64TransImage(Integer id, String type) {
        validateImageParams(id, type);

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            TransMeta transMeta = loadTransMetaForImage(type, id);
            BufferedImage image = generateImageWithIconFallback(transMeta);

            Base64.Encoder encoder = Base64.getEncoder();
            ImageIO.write(image, "png", bytes);
            return encoder.encodeToString(bytes.toByteArray());
        } catch (Exception e) {
            log.error("获取转换截图失败！ID: {}, Type: {}", id, type, e);
            throw new DataTransformationEngineException("获取转换截图失败！", e);
        }
    }

    // ==================== 私有辅助方法 - 环境配置 ====================

    /**
     * 获取插件目录
     *
     * @return 插件目录路径
     */
    private String getPluginsDir() {
        if (StringUtils.isNotEmpty(dtDesignerPluginsDir)) {
            return dtDesignerPluginsDir;
        }
        return getDefaultPluginsDir();
    }

    /**
     * 获取默认插件目录
     *
     * @return 默认插件目录路径
     */
    private String getDefaultPluginsDir() {
        // 优先使用classpath下的plugins目录
        Path classpathPluginsDir = getClasspathPluginsDir();
        if (classpathPluginsDir != null && Files.exists(classpathPluginsDir)) {
            return classpathPluginsDir.toString();
        }

        // fat-jar / fat-war 场景：classpath:plugins 位于 jar 内部，不是文件系统目录
        // 需要将 plugins/steps/** 解压到临时目录供 Kettle 扫描
        Path extractedPluginsDir = extractClasspathPluginsToTempDirIfNecessary();
        if (extractedPluginsDir != null && Files.exists(extractedPluginsDir)) {
            return extractedPluginsDir.toString();
        }

        // Spring Boot 3 / nested jar：plugins URL 可能为 jar:nested:，JarURLConnection 无法遍历，改用 classpath 扫描复制
        Path springExtracted = extractClasspathPluginsUsingSpringResources();
        if (springExtracted != null && Files.exists(springExtracted)) {
            return springExtracted.toString();
        }

        // 其次使用KETTLE_HOME下的plugins目录
        String kettleHomePluginsDir = getKettleHomePluginsDir();
        if (kettleHomePluginsDir != null) {
            return kettleHomePluginsDir;
        }

        // 回退到工作目录下的plugins
        String userDirPluginsDir = getUserDirPluginsDir();
        if (userDirPluginsDir != null) {
            return userDirPluginsDir;
        }

        // 最后兜底返回classpath位置
        return classpathPluginsDir == null ? DEFAULT_PLUGIN_DIR : classpathPluginsDir.toString();
    }

    /**
     * 获取classpath下的插件目录
     */
    private Path getClasspathPluginsDir() {
        try {
            URL root = this.getClass().getClassLoader().getResource(".");
            if (root == null) {
                return null;
            }
            if ("file".equalsIgnoreCase(root.getProtocol())) {
                String parentPath = URLDecoder.decode(root.getPath(), StandardCharsets.UTF_8.name());
                Path classpathPluginsDir = Paths.get(parentPath, DEFAULT_PLUGIN_DIR);
                return Files.exists(classpathPluginsDir) ? classpathPluginsDir : null;
            }
            // jar / nested jar：无法作为目录返回
            return null;
        } catch (UnsupportedEncodingException e) {
            log.warn("Failed to decode classpath path", e);
            return null;
        }
    }

    /**
     * 当运行在 fat-jar/fat-war 中时，classpath:plugins 不落地，Kettle 无法扫描。
     * 这里把 classpath 下的 plugins/steps/** 解压到临时目录，返回临时 plugins 根目录（包含 steps 子目录）。
     */
    private Path extractClasspathPluginsToTempDirIfNecessary() {
        try {
            URL pluginsUrl = this.getClass().getClassLoader().getResource(CLASSPATH_PLUGINS_DIR);
            if (pluginsUrl == null) {
                return null;
            }

            // file: 说明已落地（IDE / exploded war），无需解压
            if ("file".equalsIgnoreCase(pluginsUrl.getProtocol())) {
                Path p = Paths.get(pluginsUrl.toURI());
                return Files.exists(p) ? p : null;
            }

            // jar: 需要解压
            if (!"jar".equalsIgnoreCase(pluginsUrl.getProtocol())) {
                log.warn("Unsupported plugins URL protocol: {}", pluginsUrl.getProtocol());
                return null;
            }

            JarURLConnection jarConn = (JarURLConnection) pluginsUrl.openConnection();
            JarFile jarFile = jarConn.getJarFile();

            // 基于 jar 的最后修改时间做一个相对稳定的目录名，避免每次启动重复解压
            long jarLastModified = 0L;
            try {
                URI jarUri = jarConn.getJarFileURL().toURI();
                if ("file".equalsIgnoreCase(jarUri.getScheme())) {
                    jarLastModified = Files.getLastModifiedTime(Paths.get(jarUri)).toMillis();
                }
            } catch (Exception ignore) {
                // ignore
            }

            String marker = Long.toString(jarLastModified);
            Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"),
                    "pfs-kettle-plugins", marker);
            Path targetPluginsDir = baseDir.resolve(CLASSPATH_PLUGINS_DIR);
            Path targetStepsDir = targetPluginsDir.resolve("steps");

            // 已存在则直接复用
            if (Files.exists(targetStepsDir)) {
                return targetPluginsDir;
            }

            Files.createDirectories(targetStepsDir);

            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name == null || !name.startsWith(CLASSPATH_STEPS_PREFIX) || entry.isDirectory()) {
                    continue;
                }

                Path out = baseDir.resolve(name);
                Files.createDirectories(out.getParent());
                try (var in = jarFile.getInputStream(entry)) {
                    Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            log.info("Extracted kettle step plugins to '{}'", targetPluginsDir);
            return targetPluginsDir;
        } catch (Exception e) {
            log.warn("Failed to extract classpath plugins for kettle scanning", e);
            return null;
        }
    }

    /**
     * Spring Boot 3.x 常见 {@code jar:nested:...}，{@link JarURLConnection} 无法列出条目；
     * 用 Spring 扫描 {@code classpath*:plugins/steps/**} 复制到临时目录供 Kettle 扫描。
     */
    private Path extractClasspathPluginsUsingSpringResources() {
        try {
            PathMatchingResourcePatternResolver resolver =
                    new PathMatchingResourcePatternResolver(this.getClass().getClassLoader());
            Resource[] resources = resolver.getResources("classpath*:" + CLASSPATH_STEPS_PREFIX + "**/*");
            if (resources.length == 0) {
                return null;
            }
            Path base = Paths.get(
                    System.getProperty("java.io.tmpdir"),
                    "pfs-kettle-plugins",
                    "spring-res",
                    Long.toString(System.nanoTime()));
            Path pluginsRoot = base.resolve(CLASSPATH_PLUGINS_DIR);
            Path stepsRoot = pluginsRoot.resolve("steps");
            int copied = 0;
            for (Resource res : resources) {
                if (!res.isReadable()) {
                    continue;
                }
                try {
                    URL url = res.getURL();
                    String rel = relativePathUnderPluginsSteps(url);
                    if (StringUtils.isBlank(rel)) {
                        continue;
                    }
                    if (rel.endsWith("/")) {
                        Files.createDirectories(stepsRoot.resolve(rel));
                        continue;
                    }
                    Path dest = stepsRoot.resolve(rel);
                    Files.createDirectories(dest.getParent());
                    try (var in = res.getInputStream()) {
                        Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
                        copied++;
                    }
                } catch (Exception one) {
                    log.debug("Skip classpath plugin resource: {}", res, one);
                }
            }
            if (copied == 0 || !Files.exists(stepsRoot)) {
                return null;
            }
            log.info("Extracted {} kettle plugin files via classpath scan to '{}'", copied, pluginsRoot);
            return pluginsRoot;
        } catch (Exception e) {
            log.warn("Spring classpath plugin extraction failed", e);
            return null;
        }
    }

    private static String relativePathUnderPluginsSteps(URL url) {
        String u = url.toString();
        String key = CLASSPATH_STEPS_PREFIX;
        int i = u.indexOf(key);
        if (i < 0) {
            key = "plugins%2Fsteps%2F";
            i = u.indexOf(key);
            if (i < 0) {
                return null;
            }
        }
        String tail = u.substring(i + key.length());
        int cut = tail.indexOf('!');
        if (cut >= 0) {
            tail = tail.substring(0, cut);
        }
        cut = tail.indexOf('?');
        if (cut >= 0) {
            tail = tail.substring(0, cut);
        }
        try {
            tail = URLDecoder.decode(tail, StandardCharsets.UTF_8);
        } catch (Exception ignore) {
            // keep raw
        }
        while (tail.startsWith("/")) {
            tail = tail.substring(1);
        }
        return tail;
    }

    /**
     * 获取KETTLE_HOME下的插件目录
     */
    private String getKettleHomePluginsDir() {
        String kettleHome = System.getProperty("KETTLE_HOME");
        if (StringUtils.isBlank(kettleHome)) {
            return null;
        }

        Path kettleHomePluginsDir = Paths.get(kettleHome, DEFAULT_PLUGIN_DIR);
        return Files.exists(kettleHomePluginsDir) ? kettleHomePluginsDir.toString() : null;
    }

    /**
     * 获取用户目录下的插件目录
     */
    private String getUserDirPluginsDir() {
        Path userDirPluginsDir = Paths.get(System.getProperty("user.dir"), DEFAULT_PLUGIN_DIR);
        return Files.exists(userDirPluginsDir) ? userDirPluginsDir.toString() : null;
    }

    /**
     * 加载插件JAR文件
     */
    private void loadPluginJars(String pluginsDir) {
        Path pluginPath = Paths.get(pluginsDir);
        if (!Files.exists(pluginPath) || !Files.isDirectory(pluginPath)) {
            log.warn("Plugin directory does not exist: {}", pluginsDir);
            return;
        }

        try (Stream<Path> jarFiles = Files.list(pluginPath)) {
            List<Path> jars = jarFiles
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".jar"))
                    .collect(Collectors.toList());

            log.info("Found {} plugin JARs in {}", jars.size(), pluginsDir);
            jars.forEach(jar -> log.debug("Found plugin JAR: {}", jar.getFileName()));
        } catch (IOException e) {
            log.warn("Failed to scan plugin directory: {}", pluginsDir, e);
        }
    }

    // ==================== 私有辅助方法 - 转换执行 ====================

    /**
     * 验证转换流程
     */
    private void validateTransFlow(TransFlow transFlow) {
        if (transFlow == null) {
            throw new DataTransformationEngineException("转换流程不能为空");
        }
    }

    /**
     * 加载转换元数据
     */
    private TransMeta loadTransMeta(String stage, Integer id) throws Exception {
        DataFlowRepository repository = DataFlowRepository.getRepository();
        String key = buildTransKey(stage);
        TransFlowConfig transFlowConfig = repository.getTrans(key, id.toString());

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
     * 构建转换Key
     */
    private String buildTransKey(String stage) {
        return stage + "_" + Constants.TRANS;
    }

    /**
     * 创建转换包装器
     */
    private TransWrapper createTransWrapper(TransMeta transMeta) throws KettleException {
        TransWrapper trans = new TransWrapper(transMeta);
        trans.getTransMeta().setInternalKettleVariables();
        trans.copyParametersFrom(transMeta);

        trans.setMonitored(true);
        trans.setInitializing(true);
        trans.setPreparing(true);
        trans.setRunning(true);
        trans.setSafeModeEnabled(true);

        return trans;
    }

    /**
     * 设置转换参数
     */
    private List<TransParam> setupTransParameters(TransWrapper trans, List<TransParam> params) throws KettleException {
        List<TransParam> paramList = params == null ? new ArrayList<>() : new ArrayList<>(params);

        // 添加默认批次号参数
        TransParam batchIdParam = new TransParam(
                DEFAULT_BATCH_ID, trans.getInstanceId(), "", DEFAULT_BATCH_ID_NAME);
        paramList.add(batchIdParam);

        // 设置参数变量
        for (TransParam param : paramList) {
            String value = StringUtils.isEmpty(param.getValue())
                    ? param.getDefaultValue()
                    : param.getValue();

            trans.addParameterDefinition(param.getName(), value, "");

            if (RATE_LIMIT_COUNT.equals(param.getName())) {
                trans.setVariable(param.getName(), value);
            }
        }

        trans.activateParameters();
        return paramList;
    }

    /**
     * 添加参数定义到转换元数据
     */
    private void addParametersToTransMeta(TransMeta transMeta, List<TransParam> params) throws KettleException {
        for (TransParam param : params) {
            transMeta.addParameterDefinition(param.getName(), param.getValue(), param.getDescription());
        }
        transMeta.activateParameters();
    }

    /**
     * 设置监听器
     */
    private void setupListeners(TransWrapper trans, TransListener transListener,
                                StepListener stepListener, KettleStepRowListener rowListener) throws KettleException {

        if (transListener != null) {
            trans.addTransListener(transListener);
        }

        trans.prepareExecution(null);

        for (StepMetaDataCombi stepMetaDataCombi : trans.getSteps()) {
            if (rowListener != null) {
                KettleStepRowListener clonedListener = rowListener.clone();
                clonedListener.setStep(stepMetaDataCombi);
                stepMetaDataCombi.step.addRowListener(clonedListener);
            }

            if (stepListener != null) {
                stepMetaDataCombi.step.addStepListener(stepListener);
            }
        }
    }

    /**
     * 启动转换
     */
    private void startTrans(TransWrapper trans, Integer id) throws KettleException {
        TRANS_MAP.put(id, trans);
        trans.startThreads();
    }

    /**
     * 等待步骤完成
     */
    private void waitForStepsCompletion() {
        try {
            Thread.sleep(STEP_WAIT_MILLIS);
        } catch (InterruptedException e) {
            log.warn("Thread interrupted while waiting for steps to complete", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 清理转换资源
     */
    private void cleanupTransResources(TransWrapper transWrapper) {
        KettleLogStore.discardLines(transWrapper.getLogChannelId(), true);

        String instanceId = transWrapper.getInstanceId();
        JobExecutingInfo jobExecutingInfo = JobExecutingData.get(instanceId);
        if (jobExecutingInfo != null) {
            jobExecutingInfo.getJobEntryExcutinginfos().clear();
            JobExecutingData.remove(instanceId);
        }
    }

    /**
     * 创建执行异常
     */
    private DataTransformationEngineException createExecutionException(
            Exception e, TransWrapper trans, String stage, Integer id) {

        String rawMessage = StringUtils.isBlank(e.getMessage()) ? e.toString() : e.getMessage();

        StringBuilder sb = new StringBuilder();
        sb.append(rawMessage);
        sb.append("<br/><br/>[exception]=").append(e.getClass().getName());

        if (e.getCause() != null) {
            sb.append("<br/>[cause]=").append(e.getCause().getClass().getName())
                    .append(" : ").append(e.getCause().getMessage());
        }

        if (trans != null) {
            sb.append("<br/>[trans]=").append(trans.getName());
            sb.append("<br/>[logChannelId]=").append(trans.getLogChannelId());
        }

        String htmlMessage = sb.toString().replaceAll("(\r\n|\r|\n|\n\r)", "<br/>");
        log.error("执行转换失败：stage={}, id={}", stage, id, e);
        return new DataTransformationEngineException(htmlMessage, e);
    }

    // ==================== 私有辅助方法 - 图片生成 ====================

    /**
     * 验证图片参数
     */
    private void validateImageParams(Integer id, String type) {
        if (id == null) {
            throw new DataTransformationEngineException("转换ID不能为空");
        }
        if (StringUtils.isEmpty(type)) {
            throw new DataTransformationEngineException("转换类型不能为空");
        }
    }

    /**
     * 加载转换元数据用于生成图片
     */
    private TransMeta loadTransMetaForImage(String type, Integer id) throws Exception {
        DataFlowRepository repository = DataFlowRepository.getRepository();
        String key = buildTransKey(type);
        TransFlowConfig transFlowConfig = repository.getTrans(key, id.toString());

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
     * 生成图片（支持图标缺失回填）
     */
    private BufferedImage generateImageWithIconFallback(TransMeta transMeta) throws Exception {
        try {
            return generateTransformationImage(transMeta);
        } catch (Exception e) {
            if (hydrateMissingStepIcon(e)) {
                return generateTransformationImage(transMeta);
            }
            throw e;
        }
    }

    /**
     * 生成转换图片
     */
    private static BufferedImage generateTransformationImage(TransMeta transMeta) throws Exception {
        if (transMeta == null) {
            throw new IllegalArgumentException("转换元数据不能为空");
        }

        Point maximum = transMeta.getMaximum();
        maximum.multiply(IMAGE_MAGNIFICATION);

        SwingGC gc = new SwingGC(null, maximum, IMAGE_ICON_SIZE, IMAGE_GRID_SIZE, IMAGE_START_OFFSET);
        TransPainter transPainter = new TransPainter(
                gc, transMeta, maximum, null, null, null, null, null,
                new ArrayList<>(), new ArrayList<>(), IMAGE_ARROW_LENGTH, IMAGE_LINE_WIDTH,
                IMAGE_GRID_SIZE, IMAGE_START_OFFSET, true, DEFAULT_FONT_NAME, DEFAULT_FONT_SIZE);

        transPainter.setMagnification(IMAGE_MAGNIFICATION);
        transPainter.buildTransformationImage();

        return (BufferedImage) gc.getImage();
    }

    // ==================== 私有辅助方法 - 步骤图标回填 ====================

    /**
     * 回填缺失的步骤图标
     */
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

        return writeIconToFile(iconFilePath, iconBytes, pluginCode);
    }

    /**
     * 根据组件代码查找组件
     */
    private TransComponent findComponentByCode(String pluginCode) {
        LambdaQueryWrapper<TransComponent> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(TransComponent::getCode, pluginCode).last("limit 1");
        return transComponentMapper.selectOne(wrapper);
    }

    /**
     * 解码图标字节数组
     */
    private byte[] decodeIconBytes(String rawIcon) {
        String icon = rawIcon.trim();

        try {
            if (icon.startsWith(SVG_BASE64_PREFIX)) {
                return Base64.getDecoder().decode(icon.substring(SVG_BASE64_PREFIX.length()));
            }

            if (icon.matches(IMAGE_BASE64_PREFIX_PATTERN)) {
                int base64Start = icon.indexOf(";base64,") + ";base64,".length();
                return Base64.getDecoder().decode(icon.substring(base64Start));
            }

            if (icon.startsWith("<svg")) {
                return icon.getBytes(StandardCharsets.UTF_8);
            }

            return Base64.getDecoder().decode(icon);
        } catch (Exception e) {
            log.warn("Failed to decode step icon content from trans_component", e);
            return new byte[0];
        }
    }

    /**
     * 解析步骤图标文件路径
     */
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

    /**
     * 写入图标文件
     */
    private boolean writeIconToFile(Path iconFilePath, byte[] iconBytes, String pluginCode) {
        try {
            Path parent = iconFilePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(iconFilePath, iconBytes);
            log.info("Hydrated missing step icon from trans_component. pluginCode={}, iconPath={}",
                    pluginCode, iconFilePath);
            return true;
        } catch (IOException e) {
            log.warn("Failed to write hydrated step icon. pluginCode={}, iconPath={}",
                    pluginCode, iconFilePath, e);
            return false;
        }
    }

    public void setDtDesignerPluginsDir(String dtDesignerPluginsDir) {
        this.dtDesignerPluginsDir = dtDesignerPluginsDir;
    }

    public String getDtDesignerPluginsDir() {
        return dtDesignerPluginsDir;
    }

    public void setTransComponentMapper(TransComponentMapper transComponentMapper) {
        this.transComponentMapper = transComponentMapper;
    }

    /**
     * 执行类型枚举
     */
    public enum ExecutingType {
        manual,  // 手工
        timing   // 定时
    }

    /**
     * 资源类型枚举
     */
    public enum ResourceType {
        JOB,
        TRANS
    }
}