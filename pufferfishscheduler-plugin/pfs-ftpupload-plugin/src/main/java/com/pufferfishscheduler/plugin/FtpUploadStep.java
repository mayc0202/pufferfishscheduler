package com.pufferfishscheduler.plugin;

import com.pufferfishscheduler.plugin.manager.FTPManager;
import com.pufferfishscheduler.plugin.manager.FTPSManager;
import com.pufferfishscheduler.plugin.manager.FtpOption;
import com.pufferfishscheduler.plugin.common.FTPConstants;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FtpUploadStep extends BaseStep implements StepInterface {

    private FtpUploadStepMeta meta;
    private FtpUploadStepData data;

    private FTPManager ftpManager;
    private FTPSManager ftpsManager;
    private String wildcard;
    private String type;

    private Integer rowLength = 0;

    public FtpUploadStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * 流程执行启动时调用此方法
     * 方法返回true表示执行初始化成功；返回false表示初始化失败，那么转换流程将不会运行
     */
    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        //强制的：必须调用
        if (!super.init(smi, sdi)) {
            return false;
        }
        this.data = (FtpUploadStepData) sdi;
        this.meta = (FtpUploadStepMeta) smi;

        // 初始化
        try {
            initFTPConfig();
        } catch (KettleStepException e) {
            e.printStackTrace();
            dealException(e);
        }

        logDebug("DataConversion Step Init ...");
        return true;
    }

    /**
     * 初始化FTP配置
     *
     * @throws KettleStepException
     */
    private void initFTPConfig() throws KettleStepException {

        FtpOption option = new FtpOption();
        option.setHost(this.meta.getHost());
        option.setPort(this.meta.getPort().intValue());
        option.setUsername(this.meta.getUsername());
        option.setPassword(this.meta.getPassword());
        option.setTimeout(this.meta.getTimeout().intValue());
        option.setBinaryMode(Boolean.valueOf(this.meta.getBinaryMode()));
        option.setMode(this.meta.getMode());
        option.setControlEncoding(this.meta.getControlEncoding());
        wildcard = this.meta.getWildcard();

        type = this.meta.getFtpType().toUpperCase(Locale.ROOT);

        switch (type) {
            case FTPConstants.FTP:
                ftpManager = new FTPManager(option, log);
                ftpManager.init();
                break;
            case FTPConstants.FTPS:
                ftpsManager = new FTPSManager(option, log);
                ftpsManager.init();
                break;
            default:
                throw new KettleStepException(String.format("暂不支持此文件服务类型![type:%s]", type));
        }
    }

    /**
     * 处理数据
     *
     * @param smi
     * @param sdi
     * @return
     * @throws KettleException
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.data = (FtpUploadStepData) sdi;
        this.meta = (FtpUploadStepMeta) smi;

        Object[] r = this.getRow();

        // 流程结束
        if (r == null) {
            // 关闭ftp连接
            this.closeClient();
            this.setOutputDone();
            return false;
        }

        // 首次调用
        if (first) {
            first = false;
            data.outputRowMeta = this.getInputRowMeta().clone();
            rowLength = data.outputRowMeta.getValueMetaList().size();
            this.meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        }

        Object[] objects = dealRowData(r);
        if (null != objects) {
            // 复制
            Object[] outputRow = copyRowValue(r, objects);
            this.putRow(data.outputRowMeta, outputRow);
        }
        return true;
    }

    /**
     * 处理流中记录
     *
     * @param r
     * @return
     * @throws KettleStepException
     */
    private Object[] dealRowData(Object[] r) throws KettleStepException {

        // 本地和远程目录初始化
        String remoteDirectory = this.meta.getRemoteDirectory();
        String localDirectory = this.meta.getLocalDirectory();

        if (r == null) {
            if (checkDirectoryIsVariable(remoteDirectory) || checkDirectoryIsVariable(localDirectory)) {
                throw new KettleStepException("流中记录为空，请校验组件是否存在${}修饰的配置！");
            }
        } else {
            // 替换路径中的变量
            remoteDirectory = checkVariableIsExist(remoteDirectory, r);
            localDirectory = checkVariableIsExist(localDirectory, r);
        }

        // 如果适配符不为空的话
        if (StringUtils.isNotBlank(wildcard)) {
            wildcard = checkVariableIsExist(this.meta.getWildcard(), r);
        }

        // 判断localDirectory是否是一个文件或目录
        try {
            uploadFilesFromDirectory(remoteDirectory, localDirectory, r);
        } catch (KettleStepException e) {
            e.printStackTrace();
            logAndThrowException("文件上传时发生错误: ", e);
        }
        return null;
    }

    /**
     * 上传目录中的所有文件
     *
     * @param remoteDirectory 远程目录
     * @param localDirectory  本地目录
     */
    private void uploadFilesFromDirectory(String remoteDirectory, String localDirectory, Object[] r) throws KettleStepException {
        Path dirPath = Paths.get(localDirectory);

        // 校验本地上传路径是否以 \ 结尾，是就截取
        try (Stream<Path> filesStream = Files.walk(dirPath)) {
            // 使用 dirPath.resolve() 来确保路径正确构建
            Object[] fileObjects = filesStream
                    .filter(Files::isRegularFile) // 使用 Files.isRegularFile() 而不是手动检查目录
                    .toArray();

            for (Object fileObj : fileObjects) {
                Path filePath = (Path) fileObj; // 强制转换为 Path 类型
                try {
                    processFile(remoteDirectory, localDirectory, dirPath.relativize(filePath), r);
                } catch (KettleStepException e) {
                    e.printStackTrace();
                    logAndThrowException("处理文件时发生错误: " + e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logAndThrowException("遍历目录时发生错误: " + e.getMessage(), new RuntimeException(e)); // 传递原始异常作为原因
        }
    }


    /**
     * 处理每个文件或目录
     */
    private void processFile(String remoteDirectory, String localDirectory, Path file, Object[] r) throws KettleStepException {
        try {
            List<String> result = uploadFileWithWildcard(remoteDirectory, localDirectory, file);
            if (result != null) {
                Object[] output = result.toArray();
                output = (r != null) ? copyRowValue(r, output) : output;
                putRow(data.outputRowMeta, output);
            }
        } catch (Exception e) {
            logAndThrowException("处理文件失败: " + file, e);
        }
    }

    /**
     * 依据通配符上传文件
     *
     * @param remoteDirectory 远程目录
     * @param localDirectory  本地目录
     * @param file
     * @return
     * @throws KettleStepException
     */
    private List<String> uploadFileWithWildcard(String remoteDirectory,
                                                String localDirectory,
                                                Path file)
            throws KettleStepException {

        String fileName = file.getFileName().toString();

        // 通配符存在，且文件名不匹配 → 跳过
        if (!shouldProcessByWildcard(fileName)) {
            return null;
        }

        // 执行上传
        return uploadFile(remoteDirectory, localDirectory, file.toString());
    }

    /**
     * 通配符过滤逻辑
     *
     * @param fileName 文件名称
     * @return
     */
    private boolean shouldProcessByWildcard(String fileName) {
        if (wildcard == null || wildcard.isEmpty()) {
            return true; // 无通配符全部处理
        }
        return regularExpression(wildcard, fileName);
    }

    /**
     * 上传单个文件
     *
     * @param remoteDirectory
     * @param localDirectory
     * @param filePath
     * @throws IOException
     * @throws KettleStepException
     */
    private List<String> uploadFile(String remoteDirectory, String localDirectory, String filePath) throws KettleStepException {
        List<String> result = new ArrayList<>();

        // 本地目录
        String localDirectoryPath = localDirectory.replace(meta.getRootPath(), "");
        result.add(localDirectoryPath);

        // 目标目录
        result.add(remoteDirectory);

        String pathTxt = localDirectory + File.separator + filePath;

        // 文件名
        Path path = Paths.get(pathTxt);
        String fileName = path.getFileName().toString();
        String[] fileNameParts = fileName.split("\\.");
        result.add(fileNameParts[0]);

        // 文件类型
        if (fileNameParts.length > 1) {
            result.add(fileNameParts[fileNameParts.length - 1].toUpperCase());
        } else {
            result.add("");
        }

        // 获取文件大小
        long size = 0;
        try {
            size = Files.size(path);
        } catch (IOException e) {
            e.printStackTrace();
            logAndThrowException("请校验文件是否存在：", e);
        }
        result.add(String.valueOf(size));

        // 如果文件没选择覆盖，文件名添加_(副本)
        boolean overwriteFile = this.meta.getOverwriteFile().equals("true");

        if (type.equals(FTPConstants.FTP)) {
            ftpManager.uploadSingleFileForCommonSchedule(remoteDirectory, pathTxt, true, overwriteFile);
        } else {
            ftpsManager.uploadSingleFileForCommonSchedule(remoteDirectory, pathTxt, true, overwriteFile);
        }

        // 移除本地文件
        if (this.meta.getRemoveLocalFile().equals("true")) {
            removerLocalFile(path);
        }

        return result;
    }

    /**
     * 移除本地文件
     *
     * @param path
     */
    private void removerLocalFile(Path path) throws KettleStepException {
        // 遍历文件夹
        try (Stream<Path> walk = Files.walk(path)) {
            // 逆序遍历：先删除子目录的文件，再删除目录本身
            Object[] objects = walk.filter(file -> !Files.isDirectory(file)).toArray();
            for (Object object : objects) {
                Path p = Paths.get(object.toString());
                Files.delete(p);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logAndThrowException("移除本地文件失败：", e);
        }
    }

    /**
     * 根据通配符获取文件
     *
     * @param wildcard
     * @param fileName
     * @return
     */
    private boolean regularExpression(String wildcard, String fileName) {
        Matcher matcher = Pattern.compile(wildcard).matcher(fileName);
        return matcher.find();
    }

    /**
     * 校验目录参数是否是变量形式
     *
     * @param directory
     * @return
     */
    private boolean checkDirectoryIsVariable(String directory) {
        return directory.startsWith("${") && directory.endsWith("}");
    }

    /**
     * 校验目录是否存在
     *
     * @param param
     * @param row
     * @return
     * @throws KettleStepException
     */
    private String checkVariableIsExist(String param, Object[] row) throws KettleStepException {
        // 如果 param 为 null 或空字符串，直接返回空字符串
        if (StringUtils.isEmpty(param)) {
            return "";
        }

        // 如果 param 包含 ${}，则进行变量检查
        if (checkDirectoryIsVariable(param)) {
            return resolveVariable(param, row);
        }

        // 如果 param 不包含 ${}，直接返回原始 param
        return param;
    }

    /**
     * 解析变量
     *
     * @param param
     * @param row
     * @return
     * @throws KettleStepException
     */
    private String resolveVariable(String param, Object[] row) throws KettleStepException {
        // 进行变量替换处理
        String variableName = pattern(param);

        // 获取输入行的列索引
        RowMetaInterface rowMeta = this.getInputRowMeta();

        if (rowMeta == null) {
            String errorMessage = "流中没有数据!";
            log.logError(errorMessage);
            throw new KettleStepException(errorMessage);
        }

        int index = rowMeta.indexOfValue(variableName);

        // 如果流中没有该变量，抛出异常并记录错误
        if (index == -1) {
            String errorMessage = "变量 '" + variableName + "' 在流中不存在，请检查输入数据。";
            log.logError(errorMessage);
            throw new KettleStepException(errorMessage);
        }

        // 获取变量对应的值
        Object result = row[index];

        // 如果结果为空，返回空字符串
        if (result == null) {
            return "";
        }

        // 返回变量的实际值
        return String.valueOf(result);
    }

    /**
     * 截取出${}流中字段
     *
     * @param fileDirectory
     * @return
     */
    private String pattern(String fileDirectory) {
        String regex = "\\$\\{([^}]+)\\}";
        Matcher matcher = Pattern.compile(regex).matcher(fileDirectory);
        while (matcher.find()) {
            return matcher.group(1);
        }
        return fileDirectory;
    }

    /**
     * 统一的异常处理方法
     *
     * @param message
     * @param e
     * @throws KettleStepException
     */
    private void logAndThrowException(String message, Exception e) throws KettleStepException {
        dealException(e);
        throw new KettleStepException(message, e);
    }

    /**
     * 处理异常
     *
     * @param e
     */
    private void dealException(Exception e) {
        log.logError(String.format("FTP上传异常：%s", e.getMessage()));
        setErrors(1);
        setOutputDone();
        stopAll();
    }

    /**
     * 复制
     *
     * @param row
     * @param cache
     * @return
     */
    private Object[] copyRowValue(Object[] row, Object[] cache) {
        int size = this.meta.getOutputFieldList().length;
        Object[] output = Arrays.copyOf(row, rowLength + size);

        // 如果cache有效，复制数据，否则填充null
        if (cache != null && cache.length == size) {
            System.arraycopy(cache, 0, output, rowLength, size);
        } else {
            Arrays.fill(output, rowLength, rowLength + size, null);
        }

        return output;
    }

    /**
     * 步骤组件执行完成（不管成功还是失败，或者异常）后，设计器调用此方法释放资源。譬如：释放文件句柄、数据库连接等
     */
    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
        logDebug("Groovy Step Dispose ...");
    }

    /**
     * 停止客户端连接
     *
     * @param smi
     * @param sdi
     */
    @Override
    public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = (FtpUploadStepMeta) smi;
        this.data = (FtpUploadStepData) sdi;

        this.closeClient();
    }

    /**
     * 关闭客户端
     */
    private void closeClient() {
        // 关闭ftp连接
        if (ftpManager != null) {
            ftpManager.close();
        }
        // 关闭ftps连接
        if (ftpsManager != null) {
            ftpsManager.close();
        }
    }
}
