package com.pufferfishscheduler.worker.task.trans.engine;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingData;
import com.pufferfishscheduler.worker.task.trans.engine.entity.JobExecutingInfo;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransParam;
import com.pufferfishscheduler.worker.task.trans.engine.exception.DataTransformationEngineException;
import com.pufferfishscheduler.worker.task.trans.engine.listener.KettleStepRowListener;
import com.pufferfishscheduler.worker.task.trans.service.TransTaskLogService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.SwingGC;
import org.pentaho.di.core.logging.*;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainter;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 数据转换引擎
 *
 * @author Mayc
 * @since 2026-03-04 17:44
 */
@Slf4j
public class DataTransEngine {

    /**
     * 默认批次号
     */
    public static final String DEFAULT_BATCHID = "instanceId";// 默认批次号
    /**
     * 默认批次号名称
     */
    public static final String DEFAULT_BATCHID_NAME = "执行转换批次号。【系统内置参数】";
    /**
     * 默认插件目录（Kettle 会在其下追加 {@code steps} 等子目录扫描 plugin.xml）
     */
    public static final String DEFAULT_PLUGIN_DIR = "plugins";
    private static final String CLASSPATH_PLUGINS_DIR = DEFAULT_PLUGIN_DIR;
    private static final String CLASSPATH_STEPS_PREFIX = CLASSPATH_PLUGINS_DIR + "/steps/";
    /**
     * 限流次数参数名
     */
    public static final String RATE_LIMIT_COUNT = "RATE_LIMIT_COUNT";
    /**
     * DTDesigner插件目录
     */
    private String dtDesignerPluginsDir;
    /**
     * 运行中的转换实例（键为单次执行的 {@link TransWrapper#getInstanceId()}，避免多任务共用同一 flowId 时互相覆盖）
     */
    public static final Map<String, TransWrapper> CACHE_TRANS_MAP = new ConcurrentHashMap<>();
    /**
     * 存放执行中的转换日志,针对写多读少的情况使用线程安全的ConcurrentLinkedDeque来保存日志
     */
    public static Map<String, ConcurrentLinkedDeque<KettleLoggingEvent>> CACHE_TRANS_MAP_LOG = new ConcurrentHashMap<>();
    public static Map<String, TransWrapper> LOG_CHANNELS = new ConcurrentHashMap<>();

    /**
     * 最大日志行数
     */
    @Getter
    @Setter
    private int maxLogNumber;

    /**
     * 任务执行日志服务
     */
    @Getter
    @Setter
    private TransTaskLogService transTaskLogService;

    private volatile boolean kettleInitialized;
    private final Object kettleInitMonitor = new Object();

    /**
     * 初始化引擎（幂等；避免重复注册 Kettle 日志监听器）
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
                String pluginsDir = getPluginsDir();
                log.info(String.format("Load plugins from '%s'", pluginsDir));
                System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", pluginsDir);
                System.setProperty("KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "Y");
                KettleEnvironment.init(false);

                initTransEventLogListener();

                loadPluginJars(pluginsDir);

                log.info("Kettle environment initialized successfully.");
                kettleInitialized = true;
            } catch (Exception e) {
                throw new DataTransformationEngineException("初始化引擎失败！", e);
            }
        }
    }

    /**
     * 初始化转换事件日志监听器
     */
    private void initTransEventLogListener() {

        KettleLogStore.getAppender().addLoggingEventListener(new KettleLoggingEventListener() {
            @Override
            public void eventAdded(KettleLoggingEvent event) {
                if (event.getMessage() instanceof LogMessage) {
                    LogMessage lm = (LogMessage) event.getMessage();

                    TransWrapper trans = getTransByLogChannelId(lm.getLogChannelId());
                    if (trans == null) {
                        return;
                    }

                    String runKey = trans.getInstanceId();
                    if (StringUtils.isBlank(runKey)) {
                        return;
                    }

                    ConcurrentLinkedDeque<KettleLoggingEvent> logList = CACHE_TRANS_MAP_LOG.computeIfAbsent(
                            runKey, k -> new ConcurrentLinkedDeque<>());

                    synchronized (logList) {
                        if (logList.size() >= maxLogNumber) {
                            logList.poll();
                        }

                        logList.add(event);
                    }
                }
            }

        });

    }

    /**
     * 根据日志通道ID获取转换包装器
     *
     * @param logChannelId 日志通道ID
     * @return 转换包装器
     */
    private static TransWrapper getTransByLogChannelId(String logChannelId) {
        if (StringUtils.isBlank(logChannelId)) {
            return null;
        }
        if (LOG_CHANNELS.containsKey(logChannelId)) {
            return LOG_CHANNELS.get(logChannelId);
        }

        LoggingObjectInterface loggingObject = LoggingRegistry.getInstance().getLoggingObject(logChannelId);
        if (loggingObject == null) {
            return null;
        }
        LoggingObjectInterface lo = loggingObject.getParent();
        if (lo == null) {
            return null;
        }
        String parentChannelId = lo.getLogChannelId();
        if (StringUtils.isBlank(parentChannelId) || parentChannelId.equals(logChannelId)) {
            return null;
        }

        return getTransByLogChannelId(parentChannelId);
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
        // 与 master 保持一致：强制使用应用自带 classpath plugins，避免外部目录缺插件导致 plugin missing
        return getDefaultPluginsDir();
    }

    /**
     * 与 master 一致：优先 classpath 落地目录；Spring Boot fat-jar 下将 {@code plugins/steps/**} 解压到临时目录供 Kettle 扫描。
     */
    private String getDefaultPluginsDir() {
        Path classpathPluginsDir = getClasspathPluginsDir();
        if (classpathPluginsDir != null && Files.exists(classpathPluginsDir)) {
            return classpathPluginsDir.toString();
        }

        Path extractedPluginsDir = extractClasspathPluginsToTempDirIfNecessary();
        if (extractedPluginsDir != null && Files.exists(extractedPluginsDir)) {
            return extractedPluginsDir.toString();
        }

        Path springExtracted = extractClasspathPluginsUsingSpringResources();
        if (springExtracted != null && Files.exists(springExtracted)) {
            return springExtracted.toString();
        }

        String kettleHomePluginsDir = getKettleHomePluginsDir();
        if (kettleHomePluginsDir != null) {
            return kettleHomePluginsDir;
        }

        String userDirPluginsDir = getUserDirPluginsDir();
        if (userDirPluginsDir != null) {
            return userDirPluginsDir;
        }

        return classpathPluginsDir == null ? DEFAULT_PLUGIN_DIR : classpathPluginsDir.toString();
    }

    private Path getClasspathPluginsDir() {
        try {
            URL root = this.getClass().getClassLoader().getResource(".");
            if (root == null) {
                return null;
            }
            if ("file".equalsIgnoreCase(root.getProtocol())) {
                String parentPath = URLDecoder.decode(root.getPath(), StandardCharsets.UTF_8.name());
                Path dir = Paths.get(parentPath, DEFAULT_PLUGIN_DIR);
                return Files.exists(dir) ? dir : null;
            }
            return null;
        } catch (Exception e) {
            log.warn("Failed to resolve classpath plugins directory", e);
            return null;
        }
    }

    private Path extractClasspathPluginsToTempDirIfNecessary() {
        try {
            URL pluginsUrl = this.getClass().getClassLoader().getResource(CLASSPATH_PLUGINS_DIR);
            if (pluginsUrl == null) {
                return null;
            }

            if ("file".equalsIgnoreCase(pluginsUrl.getProtocol())) {
                Path p = Paths.get(pluginsUrl.toURI());
                return Files.exists(p) ? p : null;
            }

            if (!"jar".equalsIgnoreCase(pluginsUrl.getProtocol())) {
                log.warn("Unsupported plugins URL protocol: {} (url={})", pluginsUrl.getProtocol(), pluginsUrl);
                return null;
            }

            JarURLConnection jarConn = (JarURLConnection) pluginsUrl.openConnection();
            JarFile jarFile = jarConn.getJarFile();

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
            Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"), "pfs-kettle-plugins", marker);
            Path targetPluginsDir = baseDir.resolve(CLASSPATH_PLUGINS_DIR);
            Path targetStepsDir = targetPluginsDir.resolve("steps");

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
     * Spring Boot 3.x fat-jar 常为 {@code jar:nested:...}，{@link JarURLConnection} 无法遍历；
     * 用 Spring 的 classpath 扫描把 {@code plugins/steps/**} 复制到临时目录供 Kettle 扫描。
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

    private String getKettleHomePluginsDir() {
        String kettleHome = System.getProperty("KETTLE_HOME");
        if (StringUtils.isBlank(kettleHome)) {
            return null;
        }
        Path kettleHomePluginsDir = Paths.get(kettleHome, DEFAULT_PLUGIN_DIR);
        return Files.exists(kettleHomePluginsDir) ? kettleHomePluginsDir.toString() : null;
    }

    private String getUserDirPluginsDir() {
        Path userDirPluginsDir = Paths.get(System.getProperty("user.dir"), DEFAULT_PLUGIN_DIR);
        return Files.exists(userDirPluginsDir) ? userDirPluginsDir.toString() : null;
    }

    /**
     * 获取转换步骤
     *
     * @param instanceId 转换ID
     * @return 步骤名称数组
     */
    public String[] getSteps(String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return new String[0];
        }
        TransWrapper transWrapper = CACHE_TRANS_MAP.get(instanceId);

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
        CACHE_TRANS_MAP.clear();
    }

    /**
     * 移除转换
     *
     * @param instanceId 转换ID
     */
    public void removeTrans(String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return;
        }
        TransWrapper transWrapper = CACHE_TRANS_MAP.get(instanceId);

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
            CACHE_TRANS_MAP.remove(instanceId);
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
        if (null != jobExecutingInfo) {
            jobExecutingInfo.getJobEntryExcutinginfos().clear();
            JobExecutingData.remove(instanceId);
        }
    }

    /**
     * 按调度任务 ID 查找当前节点上仍在内存中的转换实例（{@link #CACHE_TRANS_MAP} 的键为 instanceId，不是 taskId）。
     */
    public TransWrapper findRunningTransByTaskId(Integer taskId) {
        if (taskId == null) {
            return null;
        }
        for (TransWrapper w : CACHE_TRANS_MAP.values()) {
            if (w != null && taskId.equals(w.getTaskId())) {
                return w;
            }
        }
        return null;
    }

    /**
     * 检查转换是否存在
     *
     * @param instanceId 转换ID
     * @return 是否存在
     */
    public boolean checkTransStatus(String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return false;
        }
        return CACHE_TRANS_MAP.get(instanceId) != null;
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
     * @param instanceId 转换ID
     */
    public void stopTrans(String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return;
        }
        TransWrapper transWrapper = CACHE_TRANS_MAP.get(instanceId);
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
        return executeTrans(transFlow, null, params, transListener, stepListener, rowListener);
    }

    /**
     * 执行转换
     *
     * @param taskId 转换任务 ID（与流程定义解耦；用于业务字段与运行维度区分，可为 null）
     */
    public TransWrapper executeTrans(TransFlow transFlow,
                                     Integer taskId,
                                     List<TransParam> params,
                                     TransListener transListener,
                                     StepListener stepListener,
                                     KettleStepRowListener rowListener) throws IOException {

        if (transFlow == null) {
            throw new DataTransformationEngineException("转换流程不能为空");
        }

        String stage = transFlow.getStage();
        Integer flowId = transFlow.getId();

        try {
            // 加载转换元数据
            TransMeta transMeta = loadTransMeta(stage, flowId);

            // 创建转换
            TransWrapper trans = createTransWrapper(transMeta, flowId, taskId);

            // 设置转换执行参数
            params = setParamsOfTrans(trans, params);
            trans.setParams(params);

            // 添加参数定义到转换元数据
            addParametersToTransMeta(transMeta, params);

            // 设置监听器
            setupListeners(trans, transListener, stepListener, rowListener);

            // 启动转换
            startTrans(trans);

            return trans;
        } catch (Exception e) {
            throw new DataTransformationEngineException(
                    e.getMessage().replaceAll("(\r\n|\r|\n|\n\r)", "<br/>"));
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
     * @param flowId    转换流程定义 ID
     * @param taskId    转换任务 ID（可选）
     * @return 转换包装器
     */
    private TransWrapper createTransWrapper(TransMeta transMeta, Integer flowId, Integer taskId) {
        TransWrapper trans = new TransWrapper(transMeta);
        trans.getTransMeta().setInternalKettleVariables();
        trans.copyParametersFrom(transMeta);
        trans.setFlowId(flowId);
        trans.setTaskId(taskId);
        if (taskId != null) {
            trans.setBusinessNo(String.valueOf(taskId));
        }

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

        // 将 Kettle 日志通道映射到当前 Trans 实例，便于全局日志监听器快速解析
        String kettleLogChannelId = trans.getLogChannelId();
        if (StringUtils.isNotBlank(kettleLogChannelId)) {
            LOG_CHANNELS.put(kettleLogChannelId, trans);
        }

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
     * @throws KettleException
     */
    private void startTrans(TransWrapper trans) throws KettleException {
        CACHE_TRANS_MAP.put(trans.getInstanceId(), trans);
        trans.startThreads();
    }

    /**
     * 清理单次运行对应的转换实例、Kettle 日志通道映射与内存日志队列
     */
    public void cleanRunningTransAndLog(TransWrapper transWrapper) {
        if (transWrapper == null) {
            return;
        }
        String instanceId = transWrapper.getInstanceId();
        String kettleLogChannelId = transWrapper.getLogChannelId();
        if (StringUtils.isNotBlank(kettleLogChannelId)) {
            LOG_CHANNELS.remove(kettleLogChannelId);
        }
        if (StringUtils.isNotBlank(instanceId)) {
            CACHE_TRANS_MAP.remove(instanceId);
            CACHE_TRANS_MAP_LOG.remove(instanceId);
        }
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

            // 生成转换图片
            BufferedImage bufferedImage = generateTransformationImage(transMeta);

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

}
