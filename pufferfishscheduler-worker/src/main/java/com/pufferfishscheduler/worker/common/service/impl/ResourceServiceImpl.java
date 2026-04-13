package com.pufferfishscheduler.worker.common.service.impl;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.config.file.FilePathConfig;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.worker.common.service.DictService;
import com.pufferfishscheduler.worker.common.service.ResourceService;
import com.pufferfishscheduler.worker.task.connect.ftp.FTPManager;
import com.pufferfishscheduler.worker.task.connect.ftp.FTPSManager;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * 资源管理 Service 实现
 */
@Slf4j
@Service
public class ResourceServiceImpl implements ResourceService {

    @Autowired
    private AESUtil aesUtil;

    @Autowired
    private FilePathConfig filePathConfig;

    @Autowired
    private DictService dictService;

    @Autowired
    private DbDatabaseService databaseService;


    @Override
    public void uploadWithNames(Integer dbId, String path, List<String> fileNames) {
        if (Objects.isNull(dbId)) {
            throw new BusinessException("请校验数据源Id是否为空!");
        }
        if (StringUtils.isBlank(path)) {
            throw new BusinessException("请校验上传目录是否为空!");
        }
        if (Objects.isNull(fileNames) || fileNames.isEmpty()) {
            throw new BusinessException("请校验文件集合是否为空!");
        }

        path = filePathConfig.getFtpPath() + File.separator + handleBuiltInDirectory(path);
        DbDatabase database = getDbDatabase(dbId);

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpUploadWithNames(database, path, fileNames);
        } else {
            ftpsUploadWithNames(database, path, fileNames);
        }
    }

    /**
     * 处理内置目录路径
     *
     * @param path 路径
     * @return 处理后的路径
     */
    private String handleBuiltInDirectory(String path) {
        return path.startsWith(filePathConfig.getFtpPath())
                ? dealPath(path.replace(filePathConfig.getFtpPath(), ""))
                : dealPath(path);
    }

    /**
     * 处理路径
     *
     * @param path 路径
     * @return 处理后的路径
     */
    private String dealPath(String path) {
        if (StringUtils.isBlank(path)) return path;
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * 获取数据库配置
     *
     * @param dbId 数据库ID
     * @return 数据库配置
     */
    private DbDatabase getDbDatabase(Integer dbId) {
        DbDatabase db = databaseService.getDatabaseById(dbId);
        if (db == null) {
            throw new BusinessException("数据源不存在");
        }
        if (!Arrays.asList(Constants.FTP_TYPE.FTP, Constants.FTP_TYPE.FTPS).contains(db.getType())) {
            throw new BusinessException("数据源类型不是FTP/FTPS");
        }
        return db;
    }

    /**
     * 获取FTP管理器
     *
     * @param database 数据库
     * @return FTP管理器
     */
    private FTPManager getFtpManager(DbDatabase database) {
        FTPManager ftpManager = new FTPManager();
        ftpManager.setHost(database.getDbHost());
        ftpManager.setPort(Integer.parseInt(database.getDbPort()));
        ftpManager.setUser(database.getUsername());
        ftpManager.setPassword(aesUtil.decrypt(database.getPassword()));

        if (StringUtils.isNotBlank(database.getProperties())) {
            JSONObject prop = JSONObject.parseObject(database.getProperties());
            ftpManager.setMode(prop.getOrDefault(Constants.FTP_PROPERTIES.MODE, Constants.MODE_TYPE.PASSIVE).toString());
            String enc = dictService.getDictItemValue(Constants.DICT.CONTROL_ENCODING,
                    prop.getOrDefault(Constants.FTP_PROPERTIES.CONTROL_ENCODING, Constants.CONTROL_ENCODING).toString());
            ftpManager.setControlEncoding(StringUtils.isNotBlank(enc) ? enc : Constants.CONTROL_ENCODING);
        } else {
            ftpManager.setMode(Constants.MODE_TYPE.PASSIVE);
            ftpManager.setControlEncoding(Constants.CONTROL_ENCODING);
        }
        return ftpManager;
    }

    /**
     * 获取FTPS管理器
     *
     * @param database 数据库
     * @return FTPS管理器
     */
    private FTPSManager getFtpsManager(DbDatabase database) {
        FTPSManager ftpsManager = new FTPSManager();
        ftpsManager.setHost(database.getDbHost());
        ftpsManager.setPort(Integer.parseInt(database.getDbPort()));
        ftpsManager.setUser(database.getUsername());
        ftpsManager.setPassword(aesUtil.decrypt(database.getPassword()));

        if (StringUtils.isNotBlank(database.getProperties())) {
            JSONObject prop = JSONObject.parseObject(database.getProperties());
            ftpsManager.setMode(prop.getOrDefault(Constants.FTP_PROPERTIES.MODE, Constants.MODE_TYPE.PASSIVE).toString());
            ftpsManager.setControlEncoding(prop.getOrDefault(Constants.FTP_PROPERTIES.CONTROL_ENCODING, Constants.CONTROL_ENCODING).toString());
        } else {
            ftpsManager.setMode(Constants.MODE_TYPE.PASSIVE);
            ftpsManager.setControlEncoding(Constants.CONTROL_ENCODING);
        }
        return ftpsManager;
    }

    /**
     * FTP上传文件
     *
     * @param database  FTP库配置
     * @param path      上传路径
     * @param fileNames 文件名列表
     */
    private void ftpUploadWithNames(DbDatabase database, String path, List<String> fileNames) {
        FTPManager ftpManager = getFtpManager(database);
        try {
            ftpManager.init();
            for (String fileName : fileNames) {
                boolean result = ftpManager.uploadFile(path, fileName, false);
                if (result) {
                    log.info("上传文件【{}】到【{}】成功！", fileName, path);
                } else {
                    log.error("上传文件【{}】到【{}】失败！", fileName, path);
                }
            }
        } catch (Exception e) {
            log.error("FTP上传异常", e);
            throw new BusinessException("上传文件失败！");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.error("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS上传文件
     *
     * @param database  FTPS库配置
     * @param path      上传路径
     * @param fileNames 文件名列表
     */
    private void ftpsUploadWithNames(DbDatabase database, String path, List<String> fileNames) {
        FTPSManager ftpsManager = getFtpsManager(database);
        try {
            ftpsManager.init();
            for (String fileName : fileNames) {
                boolean result = ftpsManager.uploadFile(path, fileName, false);
                if (result) {
                    log.info("上传文件【{}】到【{}】成功！", fileName, path);
                } else {
                    log.error("上传文件【{}】到【{}】失败！", fileName, path);
                }
            }
        } catch (Exception e) {
            log.error("FTPS上传异常", e);
            throw new BusinessException("上传文件失败！");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error("关闭FTPS失败", e);
            }
        }
    }

    /**
     * 删除本地临时目录及其所有内容
     *
     * @param localPath 本地目录绝对路径
     */
    @Override
    public void deleteLocalDirectory(String localPath) {
        if (StringUtils.isBlank(localPath)) {
            return;
        }
        try {
            File dir = new File(localPath);
            if (dir.exists()) {
                FileUtils.deleteDirectory(dir);
                log.info("成功删除临时目录: {}", localPath);
            }
        } catch (IOException e) {
            log.error("删除临时目录失败: {}", localPath, e);
        }
    }

    /**
     * 同 {@link # downloadFileToLocal(Integer, String, String, String, String, boolean)}；
     * 当 {@code trustLocalCacheWithoutFtp} 为 true 且本地已存在有效 Excel 缓存时，不再连接 FTP（用于设计器连续调用 getSheets/getFields）。
     */
    @Override
    public void downloadFileToLocal(Integer dbId, String remoteRelativeDir, String fileName, String localFullPath, boolean trustLocalCacheWithoutFtp) {
        if (dbId == null) {
            throw new BusinessException("数据源Id不能为空!");
        }
        if (StringUtils.isBlank(fileName)) {
            throw new BusinessException("文件名不能为空!");
        }
        if (StringUtils.isBlank(localFullPath)) {
            throw new BusinessException("本地保存路径不能为空!");
        }
        File localProbe = new File(localFullPath);
        if (trustLocalCacheWithoutFtp
                && localProbe.isFile()
                && localProbe.length() > 0
                && isProbableExcelOfficeFile(localProbe, fileName)) {
            log.debug("设计器预览：本地 Excel 缓存有效（{} bytes），跳过 FTP: {}", localProbe.length(), localFullPath);
            return;
        }
        DbDatabase database = getDbDatabase(dbId);
        String remotePath = StringUtils.isBlank(remoteRelativeDir)
                ? filePathConfig.getFtpPath()
                : filePathConfig.getFtpPath() + File.separator + handleBuiltInDirectory(remoteRelativeDir);

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            FTPManager ftpManager = getFtpManager(database);
            try {
                ftpManager.init();
                finishDownloadToLocal(
                        ftpManager.getFTPClient(),
                        remotePath,
                        fileName,
                        localFullPath,
                        "FTP",
                        () -> ftpManager.downloadFile(remotePath, fileName, localFullPath));
            } finally {
                try {
                    ftpManager.close();
                } catch (Exception e) {
                    log.error("关闭FTP失败", e);
                }
            }
        } else if (Constants.FTP_TYPE.FTPS.equals(database.getType())) {
            FTPSManager ftpsManager = getFtpsManager(database);
            try {
                ftpsManager.init();
                finishDownloadToLocal(
                        ftpsManager.getFTPClient(),
                        remotePath,
                        fileName,
                        localFullPath,
                        "FTPS",
                        () -> ftpsManager.downloadFile(remotePath, fileName, localFullPath));
            } finally {
                try {
                    ftpsManager.close();
                } catch (Exception e) {
                    log.error("关闭FTPS失败", e);
                }
            }
        } else {
            throw new BusinessException("请校验数据源类型是否是FTP/FTPS!");
        }
    }

    /**
     * 完成 FTP/FTPS 下载：可跳过已完整缓存；下载后校验大小与 Office 魔数，避免损坏/错误页被当成 Excel。
     */
    private void finishDownloadToLocal(
            FTPClient client,
            String remotePath,
            String fileName,
            String localFullPath,
            String protocolLabel,
            BooleanSupplier runDownload) {
        long remoteSize = safeReadRemoteRegularFileSize(client, remotePath, fileName);
        File localFile = new File(localFullPath);
        if (remoteSize >= 0 && localFile.isFile() && localFile.length() == remoteSize && remoteSize > 0) {
            if (isProbableExcelOfficeFile(localFile, fileName)) {
                log.info("本地文件已完整（{} bytes），跳过 {} 下载: {}", remoteSize, protocolLabel, localFullPath);
                return;
            }
            log.warn("本地大小与远程一致但非有效 Excel 魔数，删除后重新下载: {}", localFullPath);
            if (!localFile.delete()) {
                log.warn("删除可疑缓存失败: {}", localFullPath);
            }
        }
        if (!runDownload.getAsBoolean()) {
            throw new BusinessException("文件下载失败: " + fileName);
        }
        validateLocalFileAfterFtpDownload(localFile, fileName, remoteSize, protocolLabel);
        log.info("已下载远程文件 [{}]/[{}] 到 [{}]", remotePath, fileName, localFullPath);
    }

    private void validateLocalFileAfterFtpDownload(File localFile, String fileName, long remoteSize, String protocolLabel) {
        if (!localFile.isFile() || localFile.length() == 0) {
            throw new BusinessException(protocolLabel + " 下载后本地文件为空: " + fileName);
        }
        long localLen = localFile.length();
        if (remoteSize >= 0 && localLen != remoteSize) {
            localFile.delete();
            throw new BusinessException(String.format(
                    "%s 下载不完整（远程 %d 字节，本地 %d 字节），请重试", protocolLabel, remoteSize, localLen));
        }
        if (!isProbableExcelOfficeFile(localFile, fileName)) {
            localFile.delete();
            throw new BusinessException(protocolLabel + " 下载内容不是有效的 Excel 文件（可能损坏或为错误页），请重试");
        }
    }

    /**
     * xlsx/xlsm 为 ZIP（PK）；xls 为 OLE2 头。
     */
    private static boolean isProbableExcelOfficeFile(File f, String fileName) {
        if (f == null || !f.isFile() || f.length() < 8) {
            return false;
        }
        String lower = fileName != null ? fileName.toLowerCase(Locale.ROOT) : "";
        try (RandomAccessFile r = new RandomAccessFile(f, "r")) {
            if (lower.endsWith(".xlsx") || lower.endsWith(".xlsm") || lower.endsWith(".xlsb")) {
                return r.read() == 'P' && r.read() == 'K';
            }
            if (lower.endsWith(".xls")) {
                byte[] sig = new byte[8];
                r.readFully(sig);
                return sig[0] == (byte) 0xD0 && sig[1] == (byte) 0xCF && sig[2] == 0x11 && sig[3] == (byte) 0xE0;
            }
            r.seek(0);
            if (r.read() == 'P' && r.read() == 'K') {
                return true;
            }
            r.seek(0);
            byte[] sig = new byte[8];
            r.readFully(sig);
            return sig[0] == (byte) 0xD0 && sig[1] == (byte) 0xCF && sig[2] == 0x11 && sig[3] == (byte) 0xE0;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 查询远程普通文件大小；无法解析时返回 -1（调用方不跳过下载，由断点续传处理半成品）
     */
    private static long safeReadRemoteRegularFileSize(FTPClient client, String remotePath, String fileName) {
        try {
            if (!client.changeWorkingDirectory(remotePath)) {
                return -1;
            }
            FTPFile[] files = client.listFiles();
            if (ArrayUtils.isEmpty(files)) {
                return -1;
            }
            for (FTPFile f : files) {
                if (f != null && f.isFile() && fileName.equals(f.getName())) {
                    long sz = f.getSize();
                    return sz >= 0 ? sz : -1;
                }
            }
            return -1;
        } catch (IOException e) {
            log.warn("读取远程文件大小失败，将尝试续传/全量下载: {}/{}", remotePath, fileName, e);
            return -1;
        }
    }

}

