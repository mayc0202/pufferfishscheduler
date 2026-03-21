package com.pufferfishscheduler.master.collect.trans.plugin;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath.Feature;
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
        // if (StringUtils.isBlank(context.getId())) {
        //     throw new BusinessException("步骤ID不能为空!");
        // }
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

    // public void beforeGenerateLocalPath(Integer flowId, String stepId) {
    // String localPath = buildLocalDir(flowId, stepId);
    // createOutputDir(localPath);
    // }

    // public void afterStepForFileOutputComponent(Integer flowId, String stepId,
    // String stepConfig) {
    // JSONObject root = JSONObject.parseObject(stepConfig);
    // JSONObject data = JSONObject.parseObject(root.getString("data"),
    // Feature.OrderedField);
    // //数据源
    // String dataSourceId = data.getString("dataSource");

    // String fileSourceType = data.getString("fileSourceType");

    // if(!Constants.FILE_SOURCE_TYPE.FTP_FILE.equals(fileSourceType)){
    // return;
    // }

    // //输出文件目录，对应FTP的文件目
    // String outputPath = data.getString("outputPath");
    // String localPath = buildLocalDir(flowId, stepId);
    // List<String> fileNames = getFileNames(localPath);
    // if (CollectionUtils.isEmpty(fileNames)) {
    // return;
    // }

    // //将临时生成的文件上传到FTP目录
    // FTPService ftpService =
    // LongCloudApplicationContext.getBean(FTPService.class);
    // try {
    // ftpService.uploadFTPFiles(Integer.valueOf(dataSourceId), fileNames,
    // outputPath);
    // } finally {
    // //删除本地文件
    // delFolder(localPath);
    // }
    // }

    // /**
    // * 构造文件输出的全路径
    // *
    // * @param fileName
    // * @return
    // */
    // public String buildLocalFullPath(Integer flowId, String stepId, String
    // fileName) {
    // String localPath = buildLocalDir(flowId, stepId);
    // return localPath + fileName;
    // }

    // /**
    // * 自动创建输出目录
    // *
    // * @param fullPath
    // */
    // public void createOutputDir(String fullPath) {
    // String dirPath = fullPath.substring(0,
    // fullPath.lastIndexOf(Constants.FILE_SEPARATOR));
    // File dir = new File(dirPath);
    // if (!dir.exists()) {
    // dir.mkdirs();
    // }
    // Path path = Paths.get(fullPath);
    // Files.exists(path);
    // }

    // /**
    // * 统一路径格式，以/分隔
    // *
    // * @param path
    // * @return
    // */
    // private String formatPath(String path) {
    // if (path == null) {
    // return "";
    // }
    // if (StringUtils.isNotEmpty(path)) {
    // path = path.trim();
    // }
    // String pathNew = path.replace("\\", Constants.FILE_SEPARATOR);
    // if (!pathNew.endsWith(Constants.FILE_SEPARATOR)) {
    // return pathNew + Constants.FILE_SEPARATOR;
    // }
    // return pathNew;
    // }

    // /**
    // * 构造文件输出的全路径(不包含文件名)
    // *
    // * @return
    // */
    // private String buildLocalDir(Integer flowId, String stepId) {
    // StringBuilder fullPath = new StringBuilder();
    // //文件生成到临时目录下
    // FilePathConfig filePathConfig =
    // LongCloudApplicationContext.getBean(FilePathConfig.class);
    // fullPath.append(formatPath(filePathConfig.getFileOutputPath()));
    // String sFlowId = String.valueOf(flowId);

    // fullPath.append(sFlowId)
    // .append(Constants.FILE_SEPARATOR)
    // .append(stepId)
    // .append(Constants.FILE_SEPARATOR);
    // return fullPath.toString();
    // }

    // /**
    // * 获取所有的文件名
    // *
    // * @param path 路径
    // * @return {@link List}<{@link String}>
    // */
    // private List<String> getFileNames(String path) {
    // File file = new File(path);
    // if (!file.exists()) {
    // return null;
    // }

    // List<String> fileNames = new ArrayList<>();

    // File[] files = file.listFiles();
    // if (ArrayUtils.isEmpty(files)) {
    // return null;
    // }

    // for (File f : files) {
    // fileNames.add(path + Constants.FILE_SEPARATOR + f.getName());
    // }

    // return fileNames;
    // }

    // /**
    // * 递归删除某个目录及目录下所有的子文件和子目录
    // *
    // * @return 删除结果
    // */
    // public void delFolder(String folderPath) {
    // try {
    // delAllFiles(folderPath); //删除完里面所有内容
    // File localPath = new File(folderPath);
    // localPath.delete(); //删除空文件夹
    // } catch (Exception e) {
    // log.error("文件删除失败！",e);
    // throw new BusinessException("文件删除失败！详细错误:"+e.getMessage());
    // }

    // }

    // public static void delAllFiles(String path) {
    // File file = new File(path);
    // if (!file.exists()) {
    // return;
    // }
    // String[] tempList = file.list();
    // File tempFile = null;
    // for (String s : tempList) {
    // tempFile = new File(path + s);
    // tempFile.delete();
    // }
    // }

}
