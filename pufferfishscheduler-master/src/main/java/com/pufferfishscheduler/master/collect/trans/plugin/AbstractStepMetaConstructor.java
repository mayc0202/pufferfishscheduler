package com.pufferfishscheduler.master.collect.trans.plugin;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import com.pufferfishscheduler.master.database.resource.service.ResourceService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractStepMetaConstructor implements StepAwareInterface {

    /**
     * 创建步骤组件
     *
     * @param config    流程配置json
     * @param transMeta 转换元数据
     * @param context   上下文参数
     * @return
     *
     */
    public abstract StepMeta create(String config, TransMeta transMeta, StepContext context);

    /**
     * 步骤执行前
     */
    @Override
    public void beforeStep(Integer flowId, String stepId, String stepConfig, List<TransParam> params) {

    }

    /**
     * 步骤执行后
     */
    @Override
    public void afterStep(Integer flowId, String stepId, String stepConfig) {

    }

    /**
     * 验证输入参数
     *
     * @param config  流程配置
     * @param context 上下文
     */
    public void validateInput(String config, StepContext context) {
        if (StringUtils.isBlank(config)) {
            throw new BusinessException("流程配置不能为空!");
        }
        if (context == null) {
            throw new BusinessException("上下文参数不能为空!");
        }
        if (context.getRegistryID() == null) {
            throw new BusinessException("插件注册表不能为空!");
        }
        if (context.getStepMetaMap() == null) {
            throw new BusinessException("步骤元数据映射不能为空!");
        }
        if (context.getFlowId() == null) {
            throw new BusinessException("流程ID不能为空!");
        }
    }

    /**
     * 空值校验
     *
     * @param value        值
     * @param stepName     步骤名称
     * @param propertyName 属性名称
     */
    public void validateBlank(String value, String stepName, String propertyName) {
        if (StringUtils.isBlank(value)) {
            throw new BusinessException(String.format("步骤%s中的%s不能为空！", stepName, propertyName));
        }
    }

    /**
     * 生成本地文件路径
     *
     * @param flowId 流程ID
     * @param stepId 步骤ID
     */
    public void beforeGenerateLocalPath(Integer flowId, String stepId) {
        String localPath = buildLocalDir(flowId, stepId);
        createOutputDir(localPath);
    }

    /**
     * 解析步骤配置：兼容「仅 data 对象」与「带 name/data 外层」两种 JSON
     */
    protected JSONObject parseStepDataObject(String stepConfig) {
        if (StringUtils.isBlank(stepConfig)) {
            return null;
        }
        JSONObject root = JSONObject.parseObject(stepConfig);
        if (root == null) {
            return null;
        }
        if (root.containsKey("data")) {
            Object nested = root.get("data");
            if (nested instanceof JSONObject jo) {
                return jo;
            }
            if (nested instanceof String s && StringUtils.isNotBlank(s)) {
                return JSONObject.parseObject(s);
            }
        }
        return root;
    }

    /**
     * 设计器预览等场景下 StepContext 的步骤标识（与 FTP 本地路径无关；FTP Excel 缓存目录仅按流程 ID 划分）
     */
    public static final String EXCEL_PREVIEW_STEP_ID = "excel-preview";

    /**
     * FTP Excel 输入本地缓存目录：仅按流程 ID，{@code inputPath/flowId/}。同流程重复拉取时命中同一文件，由定时任务清理。
     */
    protected String buildFtpExcelLocalInputDir(Integer flowId) {
        FilePathConfig filePathConfig = PufferfishSchedulerApplicationContext.getBean(FilePathConfig.class);
        StringBuilder fullPath = new StringBuilder();
        fullPath.append(formatPath(filePathConfig.getInputPath()));
        fullPath.append(flowId).append(Constants.FILE_SEPARATOR);
        return fullPath.toString();
    }

    /**
     * Kettle 读取用的本地 Excel 完整路径（FTP 源下载落盘路径）
     */
    protected String buildFtpExcelLocalInputFullPath(Integer flowId, String fileName) {
        return buildFtpExcelLocalInputDir(flowId) + fileName;
    }

    /**
     * 创建 {@link #buildFtpExcelLocalInputDir(Integer)} 目录
     */
    protected void beforeGenerateFtpExcelInputDir(Integer flowId) {
        File dir = new File(buildFtpExcelLocalInputDir(flowId));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new BusinessException("创建本地输入临时目录失败: " + dir.getAbsolutePath());
        }
    }

    /**
     * 执行后处理
     */
    public void afterStepForFileOutputComponent(Integer flowId, String stepId, String stepConfig) {
        JSONObject data = parseStepDataObject(stepConfig);
        if (data == null) {
            return;
        }
        Integer dataSourceId = data.getInteger("dataSourceId");
        if (dataSourceId == null && StringUtils.isNotBlank(data.getString("dataSourceId"))) {
            dataSourceId = Integer.valueOf(data.getString("dataSourceId").trim());
        }

        String fileSourceType = data.getString("fileSourceType");

        if (!Constants.FILE_SOURCE_TYPE.FTP_FILE.equals(fileSourceType)) {
            return;
        }
        if (dataSourceId == null) {
            return;
        }

        // 输出文件目录，对应FTP的文件目
        String outputPath = data.getString("outputPath");
        String localPath = buildLocalDir(flowId, stepId);
        List<String> fileNames = getFileNames(localPath);
        ResourceService resourceService = PufferfishSchedulerApplicationContext.getBean(ResourceService.class);
        if (CollectionUtils.isEmpty(fileNames)) {
            resourceService.deleteLocalDirectory(localPath);
            return;
        }

        //将临时生成的文件上传到FTP目录
        try {
            resourceService.uploadWithNames(dataSourceId, outputPath, fileNames);
        } finally {
            // 无论上传成功或失败，都清理本地临时目录
            resourceService.deleteLocalDirectory(localPath);
        }
    }

    /**
     * 构造文件输出的全路径
     *
     * @param fileName
     * @return
     */
    public String buildLocalFullPath(Integer flowId, String stepId, String
            fileName) {
        String localPath = buildLocalDir(flowId, stepId);
        return localPath + fileName;
    }

    /**
     * 自动创建输出目录
     *
     * @param fullPath 完整路径
     */
    public void createOutputDir(String fullPath) {
        String dirPath = fullPath.substring(0,
                fullPath.lastIndexOf(Constants.FILE_SEPARATOR));
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        Path path = Paths.get(fullPath);
        Files.exists(path);
    }

    /**
     * 统一路径格式，以/分隔
     *
     * @param path
     * @return
     */
    private String formatPath(String path) {
        if (path == null) {
            return "";
        }
        if (StringUtils.isNotEmpty(path)) {
            path = path.trim();
        }
        String pathNew = path.replace("\\", Constants.FILE_SEPARATOR);
        if (!pathNew.endsWith(Constants.FILE_SEPARATOR)) {
            return pathNew + Constants.FILE_SEPARATOR;
        }
        return pathNew;
    }

    /**
     * 构造文件输出的全路径(不包含文件名)
     *
     * @return
     */
    private String buildLocalDir(Integer flowId, String stepId) {
        StringBuilder fullPath = new StringBuilder();
        //文件生成到临时目录下
        FilePathConfig filePathConfig = PufferfishSchedulerApplicationContext.getBean(FilePathConfig.class);
        fullPath.append(formatPath(filePathConfig.getOutputPath()));
        String sFlowId = String.valueOf(flowId);

        fullPath.append(sFlowId)
                .append(Constants.FILE_SEPARATOR)
                .append(stepId)
                .append(Constants.FILE_SEPARATOR);
        return fullPath.toString();
    }

    /**
     * 获取所有的文件名
     *
     * @param path 路径
     * @return {@link List}<{@link String}>
     */
    private List<String> getFileNames(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }

        List<String> fileNames = new ArrayList<>();

        File[] files = file.listFiles();
        if (ArrayUtils.isEmpty(files)) {
            return null;
        }

        for (File f : files) {
            fileNames.add(path + Constants.FILE_SEPARATOR + f.getName());
        }

        return fileNames;
    }

}
