package com.pufferfishscheduler.plugin.manager;


import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.DGPCopyStreamListener;
import com.pufferfishscheduler.plugin.common.FTPConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.*;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * https://commons.apache.org/proper/commons-net/apidocs/org/apache/commons/net/ftp/FTPSClient.html
 */

public class FTPSManager {

    private LogChannelInterface log;

    private FTPSClient ftpClient;
    private String host;
    private String controlEncoding;
    private String mode;
    private int port;
    private String user;
    private String password;
    private boolean binaryMode;                         // 二进制
    private int timeout;                                // 超时时间
    private int bufferSize = 1024 * 10 * 10;
    private static final int RETRY_TIMES_READ_DATA = 6;
    private static final int RETRY_TIMES_INIT = 3;

    /**
     * 设置连接ftp服务器超时时间。包括连接ftp服务器（connect），读取ftp服务器数据时新建的连接的超时时间。默认1分钟连接超时时间
     */
    private int connectTimeout = 60000;

    /**
     * 设置连接ftp服务器（connect）是，读取返回数据超时时间。默认1分钟连接超时时间
     */
    private int connectSoTimeout = 60000;

    /**
     * 读取文件ftp服务器文件超时时间。注意这里是执行read()方法的超时时间，不是读取这个文件的超时时间。默认2分钟
     */
    private int dataSoTimeout = 120000;

    private boolean isPassiveMode = true;//是否是被动接收文件模式

    private static final String TLS = "TLS";

    public FTPSManager() {

    }

    public FTPSManager(String host, int port, String user, String password, String controlEncoding, String mode) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.controlEncoding = controlEncoding;
        this.mode = mode;

    }

    public FTPSManager(FtpOption option, LogChannelInterface log) {
        this.host = option.getHost();
        this.port = option.getPort();
        this.user = option.getUsername();
        this.password = option.getPassword();
        this.timeout = option.getTimeout();
        this.binaryMode = option.getBinaryMode();
        this.controlEncoding = option.getControlEncoding();
        this.mode = option.getMode();
        this.log = log;
    }

    public void init() throws KettleStepException {
        init(1);
    }

    public void init(int trytimes) throws KettleStepException {
        if (trytimes > RETRY_TIMES_INIT) {
            log.logError("ftp登录失败！");
            throw new KettleStepException("ftp登录失败！");
        }

        try {
            getFTPClient();
        } catch (Exception e) {
            log.logDebug("重试登录FTP，重试第" + trytimes + "次");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            init(trytimes + 1);
        }
    }

    public FTPClient getFTPClient() throws KettleStepException {
        if ((this.ftpClient != null) && (this.ftpClient.isConnected())) {
            return this.ftpClient;
        }

        this.ftpClient = new FTPSClient();
        try {
            //创建SSL上下文
            SSLContext sslContext = SSLContext.getInstance(TLS);
            //自定义证书，忽略已过期证书
            TrustManager[] trustAllCerts = new TrustManager[1];
            TrustManager tm = new CustomeTrustManager();
            trustAllCerts[0] = tm;
            //初始化
            sslContext.init(null, trustAllCerts, null);

            this.ftpClient = new FTPSClient(true, sslContext);

            this.ftpClient.setControlEncoding(controlEncoding);
            FTPClientConfig conf = new FTPClientConfig();
            // 编码语言
            conf.setServerLanguageCode(FTPConstants.LANGUAGE_CODE);
            // 设置时区
            conf.setServerTimeZoneId(FTPConstants.DEFAULT_SERVER_TIME_ZONE_ID);
            this.ftpClient.configure(conf);

            // 设置连接超时时间
            this.ftpClient.setConnectTimeout(connectTimeout);
            // 默认超时
            this.ftpClient.setDefaultTimeout(connectSoTimeout);
            // 数据传输超时
            this.ftpClient.setDataTimeout(dataSoTimeout);
            // 设置缓冲区大小
            this.ftpClient.setBufferSize(bufferSize);
            // 设置连接保持活动
            this.ftpClient.setControlKeepAliveTimeout(60); // 每60秒发送NOOP保持连接
            this.ftpClient.setCopyStreamListener(new DGPCopyStreamListener());

            //连接
            this.ftpClient.connect(this.host, this.port);
            showServerReply();
            int replyCode = ftpClient.getReplyCode();
            if (!(FTPReply.isPositiveCompletion(replyCode))) {
                log.logError(String.format("FTP服务器连接失败。IP[%s], PORT[%s], USERNAME[%s], PASSWORD[%s]",
                        this.host, this.port, this.user, this.password));
                this.ftpClient.disconnect();
                throw new RuntimeException("FTP服务器连接失败");
            }
            //登录
            boolean bLogin = this.ftpClient.login(this.user, this.password);
            showServerReply();
            if (bLogin) {
                log.logBasic("登录FTP服务器登录成功");
            } else {
                log.logError("登录FTP服务器登录成功");
                throw new RuntimeException("登录FTP服务器登录失败");
            }
            // Set protection buffer size
            ftpClient.execPBSZ(0);
            // Set data channel protection to private
            ftpClient.execPROT("P");

            if (FTPConstants.PASSIVE.equals(mode)) {
                this.ftpClient.enterLocalPassiveMode();
            }
            if (FTPConstants.ACTIVE.equals(mode)) {
                this.ftpClient.enterLocalActiveMode();
            }
            //有防火墙的服务器修改
            if (binaryMode) {
                this.ftpClient.setFileType(FTP.BINARY_FILE_TYPE); // 文件类型
            }
            this.ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);

        } catch (Exception ex) {
            log.logError("连接FTP服务器出现异常", ex);
            throw new KettleStepException("FTP服务器登录失败!" + ex.getMessage());
        }
        return this.ftpClient;
    }

    private void showServerReply() {
        String[] replies = ftpClient.getReplyStrings();
        if (replies != null && replies.length > 0) {
            for (String aReply : replies) {
                System.out.println("SERVER: " + aReply);
            }
        }
    }

    public boolean uploadSingleFile(String remoteDir, String localFile, boolean createSubDirFlag, boolean isPassiveMode, StringBuffer ftpErrorMsg) throws KettleStepException {
        log.logDebug(String.format("上传单个文件【%s】至FTP服务器【%s】", localFile, remoteDir));
        boolean flag = false;
        String errorMsg = "";
        if (StringUtils.isEmpty(remoteDir)) {
            errorMsg = String.format("远程目标文件夹参数为空【%s】", remoteDir);
            log.logError(errorMsg);
            throw new KettleStepException(errorMsg);
        }
        if (StringUtils.isEmpty(localFile)) {
            errorMsg = String.format("本地目标文件参数为空【%s】", localFile);
            log.logError(errorMsg);
            throw new KettleStepException(errorMsg);
        }

        BufferedInputStream bis = null;
        try {
            if (!this.ftpClient.changeWorkingDirectory(remoteDir)) {
                if (createSubDirFlag) {
                    log.logBasic("FTP服务器远程目录【" + remoteDir + "】不存在。创建文件夹。");
                    makeRemoteDir(remoteDir);
                } else {
                    errorMsg = String.format("FTP服务器远程目录【%s】不存在,请联系管理员创建文件夹", remoteDir);
                    log.logError(errorMsg);
                    throw new KettleStepException(errorMsg);
                }
            }
            File file = new File(localFile);
            if (file.isFile()) {
                bis = new BufferedInputStream(new FileInputStream(file));
                flag = this.ftpClient.storeFile(file.getName(), bis);
            } else {
                errorMsg = String.format("文件【%s】不存在", localFile);
                log.logError(errorMsg);
            }
        } catch (IOException e) {
            errorMsg = String.format("上传文件失败【%s】", e.getMessage());
            log.logError(e.getMessage(), e);
        } finally {
            ftpErrorMsg.append(errorMsg);
            try {
                if (null != bis) {
                    bis.close();
                }
            } catch (IOException e) {
                log.logError(e.getMessage(), e);
            }
        }
        return flag;
    }

    public void uploadSingleFileForCommonSchedule(String remoteDir,
                                                     String localFile,
                                                     boolean createSubDirFlag,
                                                     boolean overwriteFile
    ) throws KettleStepException {
        log.logDebug(String.format("上传单个文件【%s】至FTP服务器【%s】", localFile, remoteDir));
        String errorMsg = "";
        // 验证远程目录、本地
        validateRemoteDirectoryIsBlank(remoteDir);
        validateLocalFileIsBlank(localFile);

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(localFile)))) {
            // 检查远程目录
            if (!ftpClient.changeWorkingDirectory(remoteDir)) {
                if (createSubDirFlag) {
                    log.logBasic("FTP服务器远程目录【" + remoteDir + "】不存在，正在创建...");
                    makeRemoteDir(remoteDir); // 假设此方法创建了所需的目录
                } else {
                    errorMsg = String.format("FTP服务器远程目录【%s】不存在，请联系管理员创建", remoteDir);
                    log.logError(errorMsg);
                    throw new KettleStepException(errorMsg);
                }
            }

            // 上传文件
            log.logBasic("远程文件目录：" + remoteDir);
            log.logBasic("本地文件：" + localFile);

            String remoteFilePath = joinRemotePath(remoteDir, localFile);
            if (!checkFileIsExist2(new File(localFile).getName(), remoteDir) || overwriteFile) {
                if (!ftpClient.storeFile(remoteFilePath, bis)) {
                    log.logError("文件上传失败原因：" + ftpClient.getReplyString());
                }
            }

            // 设置权限
            setFilePermission(remoteFilePath);

            log.logBasic("文件上传成功：" + remoteFilePath);

        } catch (FileNotFoundException e) {
            errorMsg = "本地文件不存在：" + localFile;
            log.logError(errorMsg, e);
            throw new KettleStepException(errorMsg, e);
        } catch (IOException e) {
            errorMsg = "文件上传过程中发生错误：" + e.getMessage();
            log.logError(errorMsg, e);
            throw new KettleStepException(errorMsg, e);
        } catch (Exception e) {
            errorMsg = String.format("上传文件失败【%s】", e.getMessage());
            log.logError("", e);
            throw new KettleStepException(errorMsg);
        }
    }

    /**
     * 验证远程目录参数是否为空
     *
     * @param remoteDirectory
     * @throws KettleStepException
     */
    public void validateRemoteDirectoryIsBlank(String remoteDirectory) throws KettleStepException {
        if (StringUtils.isEmpty(remoteDirectory)) {
            String errorMsg = String.format("远程目标文件夹参数为空【%s】", remoteDirectory);
            log.logError(errorMsg);
            throw new KettleStepException(errorMsg);
        }
    }

    /**
     * 验证本地文件参数是否为空
     *
     * @param localFile
     * @throws KettleStepException
     */
    public void validateLocalFileIsBlank(String localFile) throws KettleStepException {
        if (StringUtils.isEmpty(localFile)) {
            String errorMsg = String.format("本地目标文件参数为空【%s】", localFile);
            log.logError(errorMsg);
            throw new KettleStepException(errorMsg);
        }
    }

    /**
     * 拼接远程目录文件路径
     *
     * @param remoteDir
     * @param localFile
     * @return
     */
    private String joinRemotePath(String remoteDir, String localFile) {
        String fileName = new File(localFile).getName();
        return remoteDir.endsWith(FTPConstants.FILE_SEPARATOR)
                ? remoteDir + fileName
                : remoteDir + FTPConstants.FILE_SEPARATOR + fileName;
    }

    /**
     * 设置文件权限
     * @param remoteFilePath
     * @throws IOException
     */
    private void setFilePermission(String remoteFilePath) throws IOException {
        String chmodCommand = "SITE CHMOD 755 " + remoteFilePath;
        if (ftpClient.sendSiteCommand(chmodCommand)) {
            log.logBasic(remoteFilePath + " 文件权限设置成功");
        } else {
            log.logBasic(remoteFilePath + " 文件权限设置失败：" + ftpClient.getReplyString());
        }
    }

    /**
     * 递归创建文件夹
     *
     * @param remoteDir
     * @return
     */
    public boolean makeRemoteDir(String remoteDir, String rootPath) throws KettleStepException {

        log.logDebug(String.format("递归创建FTP文件夹:%s", remoteDir));
        boolean flag = false;
        try {
            this.ftpClient.changeWorkingDirectory(rootPath);
            if (remoteDir.contains(FTPConstants.FILE_SEPARATOR)) {
                String directory = remoteDir.substring(0, remoteDir.lastIndexOf(FTPConstants.FILE_SEPARATOR) + 1);
                if (!directory.equalsIgnoreCase(FTPConstants.FILE_SEPARATOR) && !this.ftpClient.changeWorkingDirectory(directory)) {
                    int start = 0, end = 0;
                    if (directory.startsWith(FTPConstants.FILE_SEPARATOR)) {
                        start = 1;
                    }
                    end = directory.indexOf(FTPConstants.FILE_SEPARATOR, start);
                    while (true) {
                        String subDirectory = remoteDir.substring(start, end);
                        if (!this.ftpClient.changeWorkingDirectory(subDirectory)) {
                            if (this.ftpClient.makeDirectory(subDirectory)) {
                                this.ftpClient.changeWorkingDirectory(subDirectory);
                            } else {
                                log.logError(String.format("创建FTP文件夹失败:%s", subDirectory));
                                throw new KettleStepException(String.format("创建FTP文件夹失败:%s", subDirectory));
                            }
                        }
                        start = end + 1;
                        end = directory.indexOf(FTPConstants.FILE_SEPARATOR, start);
                        if (start >= end) {
                            break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.logError(String.format("创建FTP文件夹失败:%s", remoteDir));
            throw new KettleStepException(String.format("创建FTP文件夹失败:%s", remoteDir));
        }
        return flag;
    }

    public boolean makeRemoteDir(String remoteDir) throws KettleStepException {

        log.logDebug(String.format("递归创建FTP文件夹:%s", remoteDir));
        try {
            String[] subPath = remoteDir.split(FTPConstants.FILE_SEPARATOR);
            String rootPath = FTPConstants.FILE_SEPARATOR;
            this.ftpClient.changeWorkingDirectory(rootPath);
            for (String subDirectoryName : subPath) {
                if (!StringUtils.isEmpty(subDirectoryName)) {
                    rootPath = rootPath + subDirectoryName + FTPConstants.FILE_SEPARATOR;
                    if (!this.ftpClient.changeWorkingDirectory(rootPath)) {
                        this.ftpClient.makeDirectory(subDirectoryName);
                    }
                    this.ftpClient.changeWorkingDirectory(rootPath);
                }
            }
        } catch (IOException e) {
            log.logError(String.format("创建FTP文件夹失败:%s", remoteDir));
            throw new KettleStepException(String.format("创建FTP文件夹失败:%s", remoteDir));
        }
        return true;
    }

    public void downloadSingleFile(String remoteDir, String fileName, String localDir, boolean overwrite, String formattedDate) throws KettleStepException {
        for (int retryTime = 1; retryTime <= RETRY_TIMES_READ_DATA; retryTime++) {
            try {
                downloadSingleFile1(remoteDir, fileName, localDir, overwrite,formattedDate);
                break; // 正常下载，跳出循环。否则继续尝试下载。三次下载失败后，下载失败
            } catch (Exception e) {
                // 目录不存在直接抛出异常，不重试
                if (e.getMessage().contains("是否存在")) {
                    throw new KettleStepException(e.getMessage());
                }

                log.logError(String.format("文件下载失败，正在进行第%s次重试", retryTime));
                if (retryTime == RETRY_TIMES_READ_DATA) {
                    log.logError(String.format("文件%s次下载失败，退出！", retryTime));
                    log.logError("", e);
                    throw new KettleStepException(e.getMessage());
                }

                try {
                    Thread.currentThread().sleep(2000);

                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                this.close();
                this.init();
            }
        }
    }

    private void downloadSingleFile1(String remoteDir, String fileName, String localDir, boolean overwrite, String localFileName) throws KettleStepException {

//        localDir = localDir + FTPConstants.FILE_SEPARATOR;

        BufferedOutputStream buffOut = null;
        try {

            // 校验远程目录是否存在
            String newDirectory = remoteDir.replace(fileName, "");
            if (!this.ftpClient.changeWorkingDirectory(newDirectory)) {
                throw new KettleStepException("请校验远程目录" + newDirectory + "是否存在！");
            }

            // 校验指定远程目录中是否存在该文件
            checkFileIsExist(fileName, newDirectory);

            File sfile = new File(localDir);
            if (!(sfile.exists())) {
                sfile.mkdirs();
            }

            File f = new File(localFileName);
            long localFileSize = 0;
            boolean fileExists = f.exists();

            if (fileExists && !overwrite) {
                // 续传模式
                localFileSize = f.length();
                log.logDebug(String.format("文件已经存在(大小：%s)，开启断点续传...", DGPCopyStreamListener.pretty(localFileSize)));
                ftpClient.setRestartOffset(localFileSize);
                buffOut = new BufferedOutputStream(new FileOutputStream(localFileName, true));
                log.logBasic("下载模式：续传");
            } else {
                // 覆盖模式
                ftpClient.setRestartOffset(0);
                buffOut = new BufferedOutputStream(new FileOutputStream(localFileName));
                if (fileExists) {
                    log.logBasic("下载模式：覆盖（文件已存在）");
                } else {
                    log.logBasic("下载模式：新建");
                }
            }

            log.logBasic("远程文件目录：" + remoteDir);
            log.logBasic("本地文件：" + localFileName);

            if (!this.ftpClient.retrieveFile(fileName, buffOut)) {
                String reply = ftpClient.getReplyString();
                throw new KettleStepException("文件下载失败，FTP 回复: " + reply);
            }

        } catch (Exception e) {
            log.logError("文件FTP下载失败：", e);
            throw new KettleStepException("文件FTP下载失败：" + e.getMessage());
        } finally {
            try {
                if (buffOut != null)
                    buffOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 校验文件是否存在
     *
     * @param fileName
     * @param directory
     * @throws KettleStepException
     * @throws IOException
     */
    private void checkFileIsExist(String fileName, String directory) throws KettleStepException, IOException {
        boolean exist = false;
        FTPFile[] ftpFiles = ftpClient.listFiles(directory);
        for (FTPFile ftpFile : ftpFiles) {
            if (!ftpFile.isDirectory()) {
                if (ftpFile.getName().equals(fileName)) {
                    exist = true;
                    break;
                }
            }
        }
        if (!exist) {
            throw new KettleStepException(String.format("请校验%s文件是否存在当前%s远程目录中！", fileName, directory));
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param fileName
     * @param directory
     * @return
     * @throws IOException
     */
    private boolean checkFileIsExist2(String fileName, String directory) throws IOException {
        FTPFile[] ftpFiles = ftpClient.listFiles(directory);
        for (FTPFile ftpFile : ftpFiles) {
            if (!ftpFile.isDirectory()) {
                if (ftpFile.getName().equals(fileName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 移除本地文件
     *
     * @param filePath
     */
    public void removerLocalFile(String filePath, String fileName) throws KettleStepException {
        try {
            boolean delete = ftpClient.deleteFile(filePath);
            if (!delete) {
                log.logError(String.format("文件%s删除失败！", fileName));
                throw new KettleStepException(String.format("文件%s删除失败！", fileName));
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new KettleStepException(String.format("文件%s删除异常：%s", fileName, e.getMessage()));
        }
    }

    /**
     * 移动文件到其他目录
     *
     * @param fileName
     * @param wildcard
     * @param dateInFilename
     * @param format
     * @param sourceFilePath
     * @param destFilePath
     * @param mkdir
     * @throws KettleException
     * @throws IOException
     */
    public void moveFileToOtherDirectory(String fileName, String wildcard, String dateInFilename, String format,
                                         String sourceFilePath, String destFilePath, boolean mkdir, String sourceDirectory)
            throws KettleException, IOException {

        // 校验移动目录是否存在
        String currentDirectory = "";
        boolean exist = ftpClient.changeWorkingDirectory(destFilePath);
        if (!exist && !mkdir) {
            throw new KettleStepException(String.format("请校验目标目录[%s]是否存在!", destFilePath));
        } else {
            if (!sourceDirectory.contains(".")) {
                String replace = sourceFilePath.replace(sourceDirectory, "");
                int lastIndexOf = replace.lastIndexOf(FTPConstants.FILE_SEPARATOR);
                if (lastIndexOf > -1) {
                    replace = replace.substring(0, lastIndexOf);
                }
                currentDirectory = destFilePath + replace;
                makeRemoteDir(currentDirectory);
            }
        }

        Pattern pattern = null;

        if (null != wildcard && !"".equals(wildcard)) {
            pattern = Pattern.compile(wildcard);
        }

        if (pattern != null && !pattern.matcher(fileName).matches()) {
            return;
        }

        String targetFilename = returnTargetFilename(fileName, dateInFilename, format);

        boolean renamed = ftpClient.rename(sourceFilePath, currentDirectory + FTPConstants.FILE_SEPARATOR + targetFilename);
        // 获取返回值
        if (!renamed) {
            String reply = ftpClient.getReplyString();
            throw new KettleException(String.format("FTP 文件移动失败: %s", reply));
        }
    }

    /**
     * 返回目标文件名
     *
     * @param filename
     * @param dateInFilename
     * @param format
     * @return
     */
    private String returnTargetFilename(String filename, String dateInFilename, String format) {
        if (filename == null) {
            return null;
        }

        int lastIndexOfDot = filename.lastIndexOf(".");
        if (lastIndexOfDot == -1) {
            lastIndexOfDot = filename.length();
        }

        String fileName = filename.substring(0, lastIndexOfDot);
        String fileExtension = filename.substring(lastIndexOfDot);

        if (Constants.TRUE.equals(dateInFilename) && !Utils.isEmpty(format)) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            String dateStr = dateFormat.format(new Date());
            fileName = fileName + dateStr;
        }

        return fileName + fileExtension;
    }


    /**
     * 关闭ftp客户端连接
     */
    public void close() {

        if (ftpClient == null) {
            return;
        }

        try {
            // 检查连接是否仍然打开且可用
            if (ftpClient.isConnected()) {
                try {
                    // 尝试发送 NOOP 命令检查连接是否仍然活跃
                    ftpClient.sendNoOp();
                    // 如果连接仍然活跃，则执行登出
                    ftpClient.logout();
                } catch (IOException e) {
                    // 如果发送 NOOP 失败，说明连接可能已经断开
                    log.logDebug("FTP连接可能已经断开，跳过logout操作: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            log.logDebug("登出FTP时发生异常（连接可能已断开）: " + e.getMessage());
        }

        try {
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
            }
        } catch (Exception e) {
            log.logDebug("断开FTP连接出现异常", e);
        }

    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getConnectSoTimeout() {
        return connectSoTimeout;
    }

    public void setConnectSoTimeout(int connectSoTimeout) {
        this.connectSoTimeout = connectSoTimeout;
    }

    public int getDataSoTimeout() {
        return dataSoTimeout;
    }

    public void setDataSoTimeout(int dataSoTimeout) {
        this.dataSoTimeout = dataSoTimeout;
    }

    public String getHost() {
        return host;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isBinaryMode() {
        return binaryMode;
    }

    public void setBinaryMode(boolean binaryMode) {
        this.binaryMode = binaryMode;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public List<String> readFileList(String remoteDir) {
        List<String> list = new ArrayList<String>();
        try {
            ftpClient.changeWorkingDirectory(remoteDir);

            FTPFile[] fs = ftpClient.listFiles();
            for (FTPFile ff : fs) {
                if (ff.isFile()) {
                    list.add(ff.getName());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public boolean isPassiveMode() {
        return isPassiveMode;
    }

    public void setPassiveMode(boolean isPassiveMode) {
        this.isPassiveMode = isPassiveMode;
    }

    static class CustomeTrustManager implements TrustManager, X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public boolean isServerTrusted(X509Certificate[] certs) {
            return true;
        }

        public boolean isClientTrusted(X509Certificate[] certs) {
            return true;
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType)
                throws CertificateException {
            return;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType)
                throws CertificateException {
            return;
        }
    }
}
