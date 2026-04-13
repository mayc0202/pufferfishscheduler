package com.pufferfishscheduler.plugin;

import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.FTPConstants;
import com.pufferfishscheduler.plugin.manager.FTPManager;
import com.pufferfishscheduler.plugin.manager.FTPSManager;
import com.pufferfishscheduler.plugin.manager.FtpOption;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPFile;
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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FtpDownloadStep extends BaseStep implements StepInterface {

    private FtpDownloadStepMeta meta;
    private FtpDownloadStepData data;

    private FTPManager ftpManager;
    private FTPSManager ftpsManager;
    private String wildcard;
    private String type;

    private Integer rowLength = 0;

    public FtpDownloadStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
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
        this.data = (FtpDownloadStepData) sdi;
        this.meta = (FtpDownloadStepMeta) smi;

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
     * 处理数据
     *
     * @param smi
     * @param sdi
     * @return
     * @throws KettleException
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.data = (FtpDownloadStepData) sdi;
        this.meta = (FtpDownloadStepMeta) smi;

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

        // 处理流中数据
        Object[] objects = dealRowData(r);
        if (null != objects) {
            // 复制
            Object[] outputRow = copyRowValue(r, objects);
            this.putRow(data.outputRowMeta, outputRow);
        }
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

        // 判断remoteDirectory是否是一个文件或目录
        try {
            downLoadFilesFromDirectory(remoteDirectory, localDirectory, r);
        } catch (Exception e) {
            e.printStackTrace();
            logAndThrowException("文件下载失败：", e);
        }
        return null;
    }

    /**
     * 下载目录中的所有文件
     *
     * @param remoteDirectory 远程目录
     * @param localDirectory  本地目录
     */
    private void downLoadFilesFromDirectory(String remoteDirectory, String localDirectory, Object[] r) throws KettleStepException {

        // 确保远程目录路径格式正确，去掉尾部的 File.separator
        remoteDirectory = removeDirectorySuffix(remoteDirectory).replace("\\", FTPConstants.FILE_SEPARATOR);

        // 验证目录
        ftpManager.validateRemoteDirectoryIsBlank(remoteDirectory);

        try {
            FTPFile[] ftpFiles = null;
            if (type.equals(FTPConstants.FTP)) {
                ftpFiles = ftpManager.getFTPClient().listFiles(remoteDirectory);
            } else {
                ftpFiles = ftpsManager.getFTPClient().listFiles(remoteDirectory);
            }

            if (ftpFiles == null || ftpFiles.length == 0) {
                return;
            }

            for (FTPFile file : ftpFiles) {
                if (!file.isDirectory()) {

                    List<String> result = downLoadFileWithWildcard(
                            remoteDirectory,
                            file.getName(),
                            localDirectory,
                            r
                    );

                    if (result != null) {
                        Object[] output = result.toArray();
                        output = r != null ? copyRowValue(r, output) : output;
                        putRow(data.outputRowMeta, output);
                    }
                }
            }
        } catch (IOException e) {
            // 打印详细的异常日志并抛出
            logAndThrowException("遍历目录时发生错误: " + remoteDirectory, e);
        } catch (Exception e) {
            logAndThrowException("下载文件时发生错误: " + remoteDirectory, e);
        }
    }


    /**
     * 通配符匹配后下载文件
     *
     * @param remoteDirectory 远程目录
     * @param fileName        文件名称
     * @param localDirectory  本地目录
     * @param row
     * @return
     * @throws KettleException
     * @throws IOException
     */
    private List<String> downLoadFileWithWildcard(String remoteDirectory,
                                                  String fileName,
                                                  String localDirectory,
                                                  Object[] row)
            throws KettleException, IOException {

        String remotePath = buildRemotePath(remoteDirectory, fileName);

        // 有通配符 → 必须匹配
        if (StringUtils.isNotBlank(wildcard)) {
            if (!regularExpression(wildcard, fileName)) {
                return null;
            }
        }

        // 下载文件
        return downLoadFile(remotePath, localDirectory, row);
    }

    /**
     * 统一路径拼接
     *
     * @param directory
     * @param fileName
     * @return
     */
    private String buildRemotePath(String directory, String fileName) {
        return directory.endsWith(FTPConstants.FILE_SEPARATOR)
                ? directory + fileName
                : directory + FTPConstants.FILE_SEPARATOR + fileName;
    }

    /**
     * 下载单个文件
     *
     * @param remoteDirectory
     * @param localDirectory
     * @throws IOException
     * @throws KettleStepException
     */
    private List<String> downLoadFile(String remoteDirectory, String localDirectory, Object[] row) throws KettleException, IOException {
        // 获取源文件路径
        String filePath = remoteDirectory;

        filePath = filePath.replace(FTPConstants.FILE_SEPARATOR, File.separator);

        List<String> result = new ArrayList<>();

        // 数据源目录
        result.add(remoteDirectory);

        // 目标目录
        String localDirectoryPath = localDirectory.replace(meta.getRootPath(), "");
        result.add(localDirectoryPath);

        // 文件名称
        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();
        String[] fileNameParts = fileName.split("\\.");
        result.add(fileNameParts[0]);

        // 获取文件扩展名（文件类型）
        if (fileNameParts.length > 1) {
            result.add(fileNameParts[fileNameParts.length - 1].toUpperCase());
        } else {
            result.add("");
        }

        // 本地目录拼接分隔符
        if (!localDirectory.endsWith(File.separator)) {
            localDirectory += File.separator;
        }

        // 拼接名称
        String dateInFilename = this.meta.getDateInFilename();
        String timeFormat = this.meta.getDateTimeFormat();
        String localFileName = getLocalFileName(localDirectory, fileName, dateInFilename, timeFormat);

        // 下载ftp文件
        if (type.equals(FTPConstants.FTP)) {
            ftpManager.downloadSingleFile(remoteDirectory, fileName, localDirectory, true, localFileName);
        } else {
            ftpsManager.downloadSingleFile(remoteDirectory, fileName, localDirectory, true, localFileName);
        }

        // 删除远程目录文件、移动创建文件夹
        if (this.meta.getRemoveFile().equals(Constants.TRUE)) {
            if (type.equals(FTPConstants.FTP)) {
                ftpManager.removerLocalFile(remoteDirectory, fileName);
            } else {
                ftpsManager.removerLocalFile(remoteDirectory, fileName);
            }
        } else if (this.meta.getMoveFile().equals(Constants.TRUE)) {
            // 移动目录也有可能是${}变量
            String moveToDirectory = checkVariableIsExist(this.meta.getMoveToDirectory(), row);
            String newMoveToDirectory = removeDirectorySuffix(moveToDirectory);
            if (null == newMoveToDirectory || "".equals(newMoveToDirectory)) {
                throw new KettleStepException("移动目录不能为空！");
            }

            // 移动到文件夹不存在就创建变量
            boolean mkdir = this.meta.getCreateNewFolder().equals(Constants.TRUE);

            // 获取远程目录
            String variable = checkVariableIsExist(this.meta.getRemoteDirectory(), row);
            String sourceDirectory = removeDirectorySuffix(variable);
            if (type.equals(FTPConstants.FTP)) {
                ftpManager.moveFileToOtherDirectory(fileName, wildcard, dateInFilename, timeFormat, remoteDirectory, newMoveToDirectory, mkdir, sourceDirectory);
            } else {
                ftpsManager.moveFileToOtherDirectory(fileName, wildcard, dateInFilename, timeFormat, remoteDirectory, newMoveToDirectory, mkdir, sourceDirectory);
            }
        }

        // 下载成功后获取本地文件大小
        long size = 0;
        try {
            size = Files.size(Paths.get(localFileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        result.add(size + " B");

        return result;
    }

    /**
     * 拼接本地文件名称
     *
     * @param localDirectory
     * @param fileName
     * @return
     */
    private String getLocalFileName(String localDirectory, String fileName, String dateInFilename, String format) {
        if (StringUtils.isNotBlank(dateInFilename) && dateInFilename.equals("true") && StringUtils.isNotBlank(format)) {
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            String formattedDate = formatter.format(new Date());
            String[] split = fileName.split("\\.");
            return localDirectory + split[0] + formattedDate + "." + split[1];
        } else {
            return localDirectory + fileName;
        }
    }

    /**
     * 去除目录后缀
     *
     * @param directory
     * @return
     */
    private String removeDirectorySuffix(String directory) {
        String newDirectory = directory;
        newDirectory = newDirectory.endsWith(FTPConstants.FILE_SEPARATOR) || newDirectory.endsWith(File.separator) ?
                newDirectory.substring(0, newDirectory.length() - 1) : newDirectory;
        return newDirectory;
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
     * 校验目录是否存在于流中
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
     * 校验目录参数是否是变量形式
     *
     * @param directory
     * @return
     */
    private boolean checkDirectoryIsVariable(String directory) {
        return directory.startsWith("${") && directory.endsWith("}");
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
        log.logError(String.format("FTP下载异常：%s", e.getMessage()));
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
        this.meta = (FtpDownloadStepMeta) smi;
        this.data = (FtpDownloadStepData) sdi;

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
