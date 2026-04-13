package com.pufferfishscheduler.master.database.resource.service.impl;

import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.domain.vo.database.ResourceTreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;
import com.pufferfishscheduler.common.config.file.FilePathConfig;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import com.pufferfishscheduler.master.database.connect.ftp.FTPManager;
import com.pufferfishscheduler.master.database.connect.ftp.FTPSManager;
import com.pufferfishscheduler.master.database.resource.service.ResourceService;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.function.BooleanSupplier;

/**
 * 资源 Service实现类
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

    /**
     * 分页获取资源文件列表
     *
     * @param dbId     数据库ID
     * @param name     资源名称
     * @param path     资源路径
     * @param pageNo   页码
     * @param pageSize 每页数量
     * @return 分页结果
     */
    @Override
    public IPage<ResourceVo> list(Integer dbId, String name, String path, Integer pageNo, Integer pageSize) {
        IPage<ResourceVo> page = new Page<>(pageNo, pageSize);
        if (dbId == null) return page;

        DbDatabase database = getDbDatabase(dbId);
        List<ResourceVo> resultList;

        try {
            path = filePathConfig.getFtpPath() + handleBuiltInDirectory(path);
            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                resultList = handleFtpList(database, path, name, null);
            } else if (Constants.FTP_TYPE.FTPS.equals(database.getType())) {
                resultList = handleFtpsList(database, path, name, null);
            } else {
                throw new BusinessException("请校验数据源类型是否是FTP/FTPS!");
            }
        } catch (Exception e) {
            log.error("获取FTP目录失败：{}", e.getMessage(), e);
            throw new BusinessException("获取FTP目录失败！");
        }

        if (CollectionUtils.isEmpty(resultList)) {
            page.setTotal(0);
            page.setRecords(Collections.emptyList());
            return page;
        }

        resultList.sort(Comparator.comparing(ResourceVo::getCreatedTime).reversed());
        int total = resultList.size();
        int fromIndex = Math.min((pageNo - 1) * pageSize, total);
        int toIndex = Math.min(pageNo * pageSize, total);
        List<ResourceVo> pageList = resultList.subList(fromIndex, toIndex);

        page.setTotal(total);
        page.setRecords(pageList);
        return page;
    }

    /**
     * 获取资源列表
     *
     * @param dbId        数据库ID
     * @param path        资源路径
     * @param fileTypeSet 文件类型集合
     * @return 资源列表
     */
    @Override
    public List<ResourceVo> getResourceList(Integer dbId, String path, List<String> fileTypeSet) {
        DbDatabase database = getDbDatabase(dbId);
        List<ResourceVo> resultList;

        try {
            path = filePathConfig.getFtpPath() + handleBuiltInDirectory(path);
            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                resultList = handleFtpList(database, path, null, fileTypeSet);
            } else if (Constants.FTP_TYPE.FTPS.equals(database.getType())) {
                resultList = handleFtpsList(database, path, null, fileTypeSet);
            } else {
                throw new BusinessException("请校验数据源类型是否是FTP/FTPS!");
            }
        } catch (Exception e) {
            log.error("获取FTP目录失败：{}", e.getMessage(), e);
            throw new BusinessException("获取FTP目录失败！");
        }
        return resultList;
    }

    /**
     * 获取资源目录列表(树形结构)
     *
     * @param dbId 数据库ID
     * @param path 资源路径
     * @return 资源目录列表
     */
    @Override
    public List<ResourceTreeVo> directoryTree(Integer dbId, String path) {
        if (dbId == null) {
            throw new BusinessException("数据源ID不能为空");
        }
        if (StringUtils.isBlank(path)) {
            throw new BusinessException("路径参数不能为空");
        }

        DbDatabase database = getDbDatabase(dbId);
        FTPManager ftpManager = null;
        FTPSManager ftpsManager = null;
        FTPClient ftpClient = null;

        try {
            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                ftpManager = getFtpManager(database);
                ftpClient = ftpManager.getFTPClient();
            } else {
                ftpsManager = getFtpsManager(database);
                ftpClient = ftpsManager.getFTPClient();
            }

            String normalizedPath = normalizePath(path);
            normalizedPath = filePathConfig.getFtpPath();

            if (!ftpClient.changeWorkingDirectory(normalizedPath)) {
                throw new BusinessException("当前账号权限不足/路径不存在: " + normalizedPath);
            }

            return listDirectories(ftpClient, normalizedPath);
        } catch (Exception e) {
            log.error("获取FTP目录失败: {}", e.getMessage());
            throw new BusinessException("获取FTP目录失败: " + e.getMessage());
        } finally {
            closeResources(ftpManager, ftpsManager);
        }
    }

    /**
     * 删除本地目录
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
     * 从 FTP/FTPS 下载文件到本地
     */
    @Override
    public void downloadFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCacheWithoutFtp) {
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

    /**
     * 递归获取FTP目录列表(树形结构)
     *
     * @param ftpClient FTP客户端
     * @param basePath  基础路径
     * @return 资源目录列表
     */
    private List<ResourceTreeVo> listDirectories(FTPClient ftpClient, String basePath) throws IOException {
        List<ResourceTreeVo> resultList = new ArrayList<>();
        FTPFile[] files = ftpClient.listFiles(basePath);
        if (ArrayUtils.isEmpty(files)) {
            return resultList;
        }

        for (FTPFile file : files) {
            if (!isValidDirectory(file)) {
                continue;
            }

            ResourceTreeVo vo = new ResourceTreeVo();
            vo.setLabel(file.getName());
            String childPath = buildChildPath(basePath, file.getName());
            vo.setValue(childPath);

            List<ResourceTreeVo> children = listDirectories(ftpClient, childPath);
            if (!children.isEmpty()) {
                vo.setChildren(children);
            }
            resultList.add(vo);
        }
        return resultList;
    }

    /**
     * 判断是否为有效目录
     *
     * @param file FTP文件
     * @return 是否为有效目录
     */
    private boolean isValidDirectory(FTPFile file) {
        return file != null &&
                file.isDirectory() &&
                !".".equals(file.getName()) &&
                !"..".equals(file.getName());
    }

    /**
     * 构建子目录路径
     *
     * @param basePath 基础路径
     * @param dirName  子目录名称
     * @return 子目录路径
     */
    private String buildChildPath(String basePath, String dirName) {
        return new File(basePath, dirName).getPath();
    }

    /**
     * 归一化路径
     *
     * @param path 路径
     * @return 归一化后的路径
     */
    private String normalizePath(String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!"/".equals(path) && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * 关闭FTP资源
     *
     * @param ftpManager  FTP管理器
     * @param ftpsManager FTPS管理器
     */
    private void closeResources(FTPManager ftpManager, FTPSManager ftpsManager) {
        try {
            if (ftpManager != null) ftpManager.close();
            if (ftpsManager != null) ftpsManager.close();
        } catch (Exception e) {
            log.warn("关闭FTP资源时出错: {}", e.getMessage());
        }
    }

    /**
     * 上传文件到FTP服务器
     *
     * @param dbId  数据源Id
     * @param path  上传目录路径
     * @param files 文件集合
     */
    @Override
    public void upload(Integer dbId, String path, List<MultipartFile> files) {
        if (Objects.isNull(dbId)) {
            throw new BusinessException("请校验数据源Id是否为空!");
        }
        if (StringUtils.isBlank(path)) {
            throw new BusinessException("请校验上传目录是否为空!");
        }
        if (Objects.isNull(files) || files.isEmpty()) {
            throw new BusinessException("请校验文件集合是否为空!");
        }

        path = filePathConfig.getFtpPath() + File.separator + handleBuiltInDirectory(path);
        DbDatabase database = getDbDatabase(dbId);

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpUpload(database, path, files);
        } else {
            ftpsUpload(database, path, files);
        }
    }

    /**
     * 上传文件到FTP服务器
     *
     * @param dbId      数据源Id
     * @param path      上传目录路径
     * @param fileNames 文件名称集合
     */
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
     * 创建目录到FTP服务器
     *
     * @param form 创建目录表单
     */
    @Override
    public void mkdir(ResourceForm form) {
        if (StringUtils.isBlank(form.getRemotePath())) {
            throw new BusinessException("请校验当前路径是否为空!");
        }
        if (StringUtils.isBlank(form.getName())) {
            throw new BusinessException("请校验目录名称是否为空!");
        }

        String name = dealPath(form.getName());
        String remotePath = filePathConfig.getFtpPath() + handleBuiltInDirectory(form.getRemotePath());
        DbDatabase database = getDbDatabase(form.getDbId());

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpMkdir(database, remotePath, name);
        } else {
            ftpsMkdir(database, remotePath, name);
        }
    }

    /**
     * 重命名文件到FTP服务器
     *
     * @param form 重命名文件表单
     */
    @Override
    public void rename(ResourceForm form) {
        if (StringUtils.isBlank(form.getPath())) {
            throw new BusinessException("请校验当前路径是否为空!");
        }
        if (StringUtils.isBlank(form.getNewName())) {
            throw new BusinessException("请校验新名称是否为空!");
        }
        if (StringUtils.isBlank(form.getOldName())) {
            throw new BusinessException("请校验旧名称是否为空!");
        }

        String oldName = handleBuiltInDirectory(form.getOldName());
        String newName = handleBuiltInDirectory(form.getNewName());
        String path = filePathConfig.getFtpPath() + File.separator + handleBuiltInDirectory(form.getPath());
        DbDatabase database = getDbDatabase(form.getDbId());

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpRename(database, path, oldName, newName);
        } else {
            ftpsRename(database, path, oldName, newName);
        }
    }

    /**
     * 移动文件到FTP服务器
     *
     * @param form 移动文件表单
     */
    @Override
    public void move(ResourceForm form) {
        if (StringUtils.isBlank(form.getFromPath())) {
            throw new BusinessException("请校验来源路径是否为空!");
        }
        if (StringUtils.isBlank(form.getToPath())) {
            throw new BusinessException("请校验目标路径是否为空!");
        }

        String fromPath = handleBuiltInDirectory(form.getFromPath());
        String toPath = handleBuiltInDirectory(form.getToPath());
        fromPath = filePathConfig.getFtpPath() + fromPath;
        toPath = filePathConfig.getFtpPath() + toPath;
        DbDatabase database = getDbDatabase(form.getDbId());

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpMove(database, form.getType(), fromPath, toPath);
        } else {
            ftpsMove(database, form.getType(), fromPath, toPath);
        }
    }

    /**
     * 移除文件到FTP服务器
     *
     * @param dbId 数据源Id
     * @param type 资源类型
     * @param path 删除的对象路径
     */
    @Override
    public void remove(Integer dbId, String type, String path) {
        if (Objects.isNull(dbId)) {
            throw new BusinessException("请校验数据源id是否为空!");
        }
        if (StringUtils.isBlank(type)) {
            throw new BusinessException("请校资源类型是否为空!");
        }
        if (StringUtils.isBlank(path)) {
            throw new BusinessException("请校验删除的对象是否为空!");
        }

        String remotePath = filePathConfig.getFtpPath() + File.separator + handleBuiltInDirectory(path);
        DbDatabase database = getDbDatabase(dbId);

        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpRemove(database, type, remotePath);
        } else {
            ftpsRemove(database, type, remotePath);
        }
    }

    /**
     * 构建路径
     *
     * @param path 路径
     * @param name 名称
     * @return 路径
     */
    private String buildPath(String path, String name) {
        if (StringUtils.isBlank(name)) {
            return path.endsWith("/") && !path.equals("/") ? path.substring(0, path.length() - 1) : path;
        }
        return new File(path, name).getPath();
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
     * 设置时区
     */
    private void setTimeZone() {
        TimeZone time = TimeZone.getTimeZone(Constants.TIME_ZONE);
        TimeZone.setDefault(time);
    }

    /**
     * 处理FTP资源列表
     *
     * @param database 数据库
     * @return 资源列表
     */
    private List<ResourceVo> handleFtpList(DbDatabase database, String path, String name, List<String> fileTypeSet) throws IOException {
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {
            if (!ftpClient.changeWorkingDirectory(path)) return Collections.emptyList();
            FTPFile[] fs = ftpClient.listFiles();
            return ArrayUtils.isNotEmpty(fs) ? buildList(fs, name, database.getId(), fileTypeSet) : Collections.emptyList();
        } finally {
            ftpManager.close();
        }
    }

    /**
     * 处理FTPS资源列表
     */
    private List<ResourceVo> handleFtpsList(DbDatabase database, String path, String name, List<String> fileTypeSet) throws IOException {
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPSClient ftpsClient = ftpsManager.getFTPClient();
        try {
            if (!ftpsClient.changeWorkingDirectory(path)) return Collections.emptyList();
            FTPFile[] fs = ftpsClient.listFiles();
            return ArrayUtils.isNotEmpty(fs) ? buildList(fs, name, database.getId(), fileTypeSet) : Collections.emptyList();
        } finally {
            ftpsManager.close();
        }
    }

    /**
     * 构建资源列表
     */
    private List<ResourceVo> buildList(FTPFile[] files, String nameFilter, Integer dbId, List<String> fileTypeSet) {
        List<ResourceVo> resultList = new ArrayList<>();
        for (FTPFile file : files) {
            String fileName = file.getName();
            if (file.isDirectory()) {
                if (".".equals(fileName) || "..".equals(fileName)) continue;
                resultList.add(buildResourceVo(file, fileName, dbId));
                continue;
            }

            if (StringUtils.isNotBlank(nameFilter) && !fileName.contains(nameFilter)) continue;
            if (!CollectionUtils.isEmpty(fileTypeSet)) {
                String ext = getFileExtension(fileName);
                if (!fileTypeSet.contains(ext)) continue;
            }
            resultList.add(buildResourceVo(file, fileName, dbId));
        }
        return resultList;
    }

    /**
     * 获取文件扩展名
     */
    private String getFileExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot == -1 || lastDot == fileName.length() - 1) return "";
        return fileName.substring(lastDot + 1).toLowerCase();
    }

    /**
     * 构建资源VO
     */
    private ResourceVo buildResourceVo(FTPFile file, String fileName, Integer dbId) {
        ResourceVo vo = new ResourceVo();
        vo.setName(fileName);
        vo.setType(file.isDirectory() ? Constants.FTP_FILE_TYPE.DIRECTORY : Constants.FTP_FILE_TYPE.FILE);
        vo.setCreatedTime(Date.from(file.getTimestamp().toInstant().atZone(ZoneId.systemDefault()).toInstant()));
        vo.setSize(file.isFile() ? formatBytes(file.getSize()) : Constants.DIRECTORY_SIZE);
        vo.setDbId(dbId);
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));
        return vo;
    }

    /**
     * 格式化字大小
     */
    private String formatBytes(long bytes) {
        if (bytes <= 0) return "0KB";
        double kb = bytes / 1024.0;
        return kb < 0.01 ? "<0.01KB" : String.format("%.2fKB", kb);
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
     * @param database FTP库配置
     * @param path     上传路径
     * @param files    文件列表
     */
    private void ftpUpload(DbDatabase database, String path, List<MultipartFile> files) {
        FTPManager ftpManager = getFtpManager(database);
        // 系统临时目录（Windows/Mac 100%有权限）
        File tempBaseDir = new File(System.getProperty("java.io.tmpdir"), "ftp_upload_" + UUID.randomUUID());

        try {
            ftpManager.init();
            // 统一创建一次临时目录
            if (!tempBaseDir.exists() && !tempBaseDir.mkdirs()) {
                throw new BusinessException("创建系统临时文件夹失败");
            }

            for (MultipartFile file : files) {
                if (file.isEmpty()) continue;
                File tempFile = new File(tempBaseDir, file.getOriginalFilename());
                FileUtils.copyInputStreamToFile(file.getInputStream(), tempFile);

                boolean result = ftpManager.uploadFile(path, tempFile.getAbsolutePath(), false);
                if (result) {
                    log.info("上传文件【{}】到【{}】成功！", file.getOriginalFilename(), path);
                } else {
                    log.error("上传文件【{}】到【{}】失败！", file.getOriginalFilename(), path);
                    throw new BusinessException("文件上传失败：" + file.getOriginalFilename());
                }
            }
        } catch (Exception e) {
            log.error("FTP上传异常", e);
            throw new BusinessException("上传文件失败！" + e.getMessage());
        } finally {
            // 统一删除临时目录
            deleteFolder(tempBaseDir);
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
     * @param database FTPS库配置
     * @param path     上传路径
     * @param files    文件列表
     */
    private void ftpsUpload(DbDatabase database, String path, List<MultipartFile> files) {
        FTPSManager ftpsManager = getFtpsManager(database);
        File tempBaseDir = new File(System.getProperty("java.io.tmpdir"), "ftps_upload_" + UUID.randomUUID());

        try {
            ftpsManager.init();
            if (!tempBaseDir.exists() && !tempBaseDir.mkdirs()) {
                throw new BusinessException("创建系统临时文件夹失败");
            }

            for (MultipartFile file : files) {
                if (file.isEmpty()) continue;
                File tempFile = new File(tempBaseDir, file.getOriginalFilename());
                FileUtils.copyInputStreamToFile(file.getInputStream(), tempFile);

                boolean result = ftpsManager.uploadFile(path, tempFile.getAbsolutePath(), false);
                if (result) {
                    log.info("上传文件【{}】到【{}】成功！", file.getOriginalFilename(), path);
                } else {
                    log.error("上传文件【{}】到【{}】失败！", file.getOriginalFilename(), path);
                    throw new BusinessException("文件上传失败：" + file.getOriginalFilename());
                }
            }
        } catch (Exception e) {
            log.error("FTPS上传异常", e);
            throw new BusinessException("上传文件失败！" + e.getMessage());
        } finally {
            deleteFolder(tempBaseDir);
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error("关闭FTPS失败", e);
            }
        }
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
     * FTP创建目录
     *
     * @param database   FTP库配置
     * @param remotePath 远程路径
     * @param name       目录名
     */
    private void ftpMkdir(DbDatabase database, String remotePath, String name) {
        setTimeZone();
        FTPManager ftpManager = getFtpManager(database);
        try {
            ftpManager.init();
            FTPClient ftpClient = ftpManager.getFTPClient();
            String path = buildPath(remotePath, name);
            ftpClient.changeWorkingDirectory(path);
            if (ftpClient.getReplyCode() != 550) {
                throw new BusinessException("目录已存在");
            }
            ftpClient.changeWorkingDirectory(remotePath);
            ftpManager.createRemoteDirectory(path);
            log.info("创建【{}】目录成功！", name);
        } catch (Exception e) {
            log.error("创建目录失败", e);
            throw new BusinessException("创建目录失败！");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.error("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS创建目录
     *
     * @param database   FTPS库配置
     * @param remotePath 远程路径
     * @param name       目录名
     */
    private void ftpsMkdir(DbDatabase database, String remotePath, String name) {
        setTimeZone();
        FTPSManager ftpsManager = getFtpsManager(database);
        try {
            ftpsManager.init();
            FTPSClient ftpsClient = ftpsManager.getFTPClient();
            String path = buildPath(remotePath, name);
            ftpsClient.changeWorkingDirectory(path);
            if (ftpsClient.getReplyCode() != 550) {
                throw new BusinessException("目录已存在");
            }
            ftpsClient.changeWorkingDirectory(remotePath);
            ftpsManager.createRemoteDirectory(path);
            log.info("创建【{}】目录成功！", name);
        } catch (BusinessException e) {
            throw new BusinessException("目录已存在，创建目录失败！");
        } catch (Exception e) {
            log.error("创建目录失败", e);
            throw new BusinessException("创建目录失败！");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error("关闭FTPS失败", e);
            }
        }
    }

    /**
     * FTP重命名文件
     *
     * @param database FTP库配置
     * @param path     文件路径
     * @param oldName  旧文件名
     * @param newName  新文件名
     */
    private void ftpRename(DbDatabase database, String path, String oldName, String newName) {
        setTimeZone();
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {
            String oldPath = buildPath(path, oldName);
            String newPath = buildPath(path, newName);
            if (!ftpClient.rename(oldPath, newPath)) {
                throw new BusinessException("重命名【" + newName + "】失败");
            }
        } catch (Exception e) {
            log.error("重命名失败", e);
            throw new BusinessException("重命名【" + newName + "】失败");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.error("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS重命名文件
     *
     * @param database FTPS库配置
     * @param path     文件路径
     * @param oldName  旧文件名
     * @param newName  新文件名
     */
    private void ftpsRename(DbDatabase database, String path, String oldName, String newName) {
        setTimeZone();
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPSClient ftpsClient = ftpsManager.getFTPClient();
        try {
            String oldPath = buildPath(path, oldName);
            String newPath = buildPath(path, newName);
            if (!ftpsClient.rename(oldPath, newPath)) {
                throw new BusinessException("重命名【" + newName + "】失败");
            }
        } catch (Exception e) {
            log.error("重命名失败", e);
            throw new BusinessException("重命名【" + newName + "】失败");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error("关闭FTPS失败", e);
            }
        }
    }

    /**
     * FTP移动文件或目录
     *
     * @param database FTP库配置
     * @param type     文件类型
     * @param fromPath 源路径
     * @param toPath   目标路径
     */
    private void ftpMove(DbDatabase database, String type, String fromPath, String toPath) {
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {
            toPath = toPath.endsWith("/") ? toPath.substring(0, toPath.length() - 1) : toPath;
            if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                if (ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录已存在");
                }
                if (!extractRootDirectory(fromPath).equals(extractRootDirectory(toPath))) {
                    throw new BusinessException("不能跨根目录移动");
                }
            } else {
                if (!ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录不存在");
                }
                String fileName = fromPath.lastIndexOf('/') > -1 ? fromPath.substring(fromPath.lastIndexOf('/') + 1) : fromPath;
                toPath += "/" + fileName;
            }

            if (!ftpClient.rename(fromPath, toPath)) {
                throw new BusinessException("移动失败：" + ftpClient.getReplyString());
            }
        } catch (Exception e) {
            log.error("移动失败", e);
            throw new BusinessException("移动失败");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.warn("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS移动文件或目录
     *
     * @param database FTPS库配置
     * @param type     文件类型
     * @param fromPath 源路径
     * @param toPath   目标路径
     */
    private void ftpsMove(DbDatabase database, String type, String fromPath, String toPath) {
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPClient ftpClient = ftpsManager.getFTPClient();
        try {
            toPath = toPath.endsWith("/") ? toPath.substring(0, toPath.length() - 1) : toPath;
            if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                if (ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录已存在");
                }
                if (!extractRootDirectory(fromPath).equals(extractRootDirectory(toPath))) {
                    throw new BusinessException("不能跨根目录移动");
                }
            } else {
                if (!ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录不存在");
                }
                String fileName = fromPath.lastIndexOf('/') > -1 ? fromPath.substring(fromPath.lastIndexOf('/') + 1) : fromPath;
                toPath += "/" + fileName;
            }

            if (!ftpClient.rename(fromPath, toPath)) {
                throw new BusinessException("移动失败：" + ftpClient.getReplyString());
            }
        } catch (Exception e) {
            log.error("移动失败", e);
            throw new BusinessException("移动失败");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.warn("关闭FTPS失败", e);
            }
        }
    }

    /**
     * 从路径中提取根目录
     *
     * @param path 路径
     * @return 根目录
     */
    private String extractRootDirectory(String path) {
        if (path == null || path.isEmpty()) return "/";
        String[] parts = path.split("/");
        return parts.length > 1 ? "/" + parts[1] : "/";
    }

    /**
     * FTP删除文件或目录
     *
     * @param database FTP库配置
     * @param type     文件类型
     * @param path     文件路径
     */
    private void ftpRemove(DbDatabase database, String type, String path) {
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {
            boolean result = false;
            if (Constants.FTP_FILE_TYPE.FILE.equals(type)) {
                result = ftpClient.deleteFile(path);
            } else if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                deleteFolder(path, ftpClient);
                result = ftpClient.removeDirectory(path);
            }
            if (!result) {
                log.warn("【{}】删除失败", path);
            }
        } catch (Exception e) {
            log.error("删除失败", e);
            throw new BusinessException("【" + path + "】删除失败");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.warn("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS删除文件或目录
     *
     * @param database FTPS库配置
     * @param type     文件类型
     * @param path     文件路径
     */
    private void ftpsRemove(DbDatabase database, String type, String path) {
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPClient ftpClient = ftpsManager.getFTPClient();
        try {
            boolean result = false;
            if (Constants.FTP_FILE_TYPE.FILE.equals(type)) {
                result = ftpClient.deleteFile(path);
            } else if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                deleteFolder(path, ftpClient);
                result = ftpClient.removeDirectory(path);
            }
            if (!result) {
                log.warn("【{}】删除失败", path);
            }
        } catch (Exception e) {
            log.error("删除失败", e);
            throw new BusinessException("【" + path + "】删除失败");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.warn("关闭FTPS失败", e);
            }
        }
    }

    /**
     * 递归删除FTP目录
     *
     * @param dirPath   目录路径
     * @param ftpClient FTP客户端
     */
    public void deleteFolder(String dirPath, FTPClient ftpClient) {
        try {
            ftpClient.changeWorkingDirectory(dirPath);
            FTPFile[] files = ftpClient.listFiles();
            if (ArrayUtils.isNotEmpty(files)) {
                for (FTPFile file : files) {
                    String filePath = new File(dirPath, file.getName()).getPath();
                    if (file.isFile()) {
                        ftpClient.deleteFile(filePath);
                    } else {
                        deleteFolder(filePath, ftpClient);
                    }
                }
            }
            ftpClient.removeDirectory(dirPath);
        } catch (Exception e) {
            log.error("递归删除FTP目录失败", e);
        }
    }

    /**
     * 递归删除本地目录
     *
     * @param file 目录文件
     */
    public void deleteFolder(File file) {
        try {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File f : files) {
                        deleteFolder(f);
                    }
                }
            }
            file.delete();
        } catch (Exception e) {
            log.warn("删除本地文件失败：{}", file.getAbsolutePath(), e);
        }
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
     * 下载文件
     *
     * @param response 响应
     * @param form     下载表单
     */
    @Override
    public void download(HttpServletResponse response, ResourceForm form) {
        if (StringUtils.isBlank(form.getName()) || StringUtils.isBlank(form.getPath())) {
            throw new BusinessException("参数不能为空");
        }
        DbDatabase database = getDbDatabase(form.getDbId());
        try {
            String fileName = form.getName();
            setDownloadHeader(response, fileName, getContentType(fileName));
            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                ftpDirectDownload(database, form, response);
            } else {
                ftpsDirectDownload(database, form, response);
            }
            log.info("文件下载完成：{}", fileName);
        } catch (Exception e) {
            log.error("下载失败", e);
            throw new BusinessException("文件下载失败：" + e.getMessage());
        }
    }

    /**
     * FTP直接下载文件
     *
     * @param database 数据库配置
     * @param form     下载表单
     * @param response 响应
     */
    private void ftpDirectDownload(DbDatabase database, ResourceForm form, HttpServletResponse response) {
        FTPManager ftpManager = getFtpManager(database);
        try {
            ftpManager.init();
            FTPClient ftpClient = ftpManager.getFTPClient();
            String remotePath = filePathConfig.getFtpPath() + "/" + handleBuiltInDirectory(form.getPath());
            String fullPath = buildPath(remotePath, form.getName());

            FTPFile[] files = ftpClient.listFiles(fullPath);
            if (ArrayUtils.isEmpty(files) || !files[0].isFile()) {
                throw new BusinessException("文件不存在");
            }

            response.setHeader("Content-Length", String.valueOf(files[0].getSize()));
            if (!ftpClient.retrieveFile(fullPath, response.getOutputStream())) {
                throw new BusinessException("下载失败");
            }
        } catch (Exception e) {
            log.error("FTP下载异常", e);
            throw new BusinessException("下载失败");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.warn("关闭FTP失败", e);
            }
        }
    }

    /**
     * FTPS直接下载文件
     *
     * @param database 数据库配置
     * @param form     下载表单
     * @param response 响应
     */
    private void ftpsDirectDownload(DbDatabase database, ResourceForm form, HttpServletResponse response) {
        FTPSManager ftpsManager = getFtpsManager(database);
        try {
            ftpsManager.init();
            FTPSClient ftpsClient = ftpsManager.getFTPClient();
            String remotePath = filePathConfig.getFtpPath() + "/" + handleBuiltInDirectory(form.getPath());
            String fullPath = buildPath(remotePath, form.getName());

            FTPFile[] files = ftpsClient.listFiles(fullPath);
            if (ArrayUtils.isEmpty(files) || !files[0].isFile()) {
                throw new BusinessException("文件不存在");
            }

            response.setHeader("Content-Length", String.valueOf(files[0].getSize()));
            if (!ftpsClient.retrieveFile(fullPath, response.getOutputStream())) {
                throw new BusinessException("下载失败");
            }
        } catch (Exception e) {
            log.error("FTPS下载异常", e);
            throw new BusinessException("下载失败");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.warn("关闭FTPS失败", e);
            }
        }
    }

    /**
     * 获取文件内容类型
     *
     * @param fileName 文件名
     * @return 文件内容类型
     */
    private String getContentType(String fileName) {
        String ext = getFileExtension(fileName);
        return switch (ext) {
            case Constants.FILE_TYPE.TXT -> Constants.CONTENT_TYPE.TXT;
            case Constants.FILE_TYPE.PDF -> Constants.CONTENT_TYPE.PDF;
            case Constants.FILE_TYPE.ZIP -> Constants.CONTENT_TYPE.ZIP;
            case Constants.FILE_TYPE.XLS, Constants.FILE_TYPE.XLSX -> Constants.CONTENT_TYPE.XLS;
            case Constants.FILE_TYPE.DOC, Constants.FILE_TYPE.DOCX -> Constants.CONTENT_TYPE.DOC;
            case Constants.FILE_TYPE.PPT, Constants.FILE_TYPE.PPTX -> Constants.CONTENT_TYPE.PPT;
            case Constants.FILE_TYPE.JPG, Constants.FILE_TYPE.JPEG -> Constants.CONTENT_TYPE.JPG;
            case Constants.FILE_TYPE.PNG -> Constants.CONTENT_TYPE.PNG;
            case Constants.FILE_TYPE.GIF -> Constants.CONTENT_TYPE.GIF;
            case Constants.FILE_TYPE.XML -> Constants.CONTENT_TYPE.XML;
            case Constants.FILE_TYPE.JSON -> Constants.CONTENT_TYPE.JSON;
            default -> Constants.CONTENT_TYPE.OCTET_STREAM;
        };
    }

    /**
     * 设置下载响应头
     *
     * @param response    响应
     * @param fileName    文件名
     * @param contentType 文件内容类型
     * @throws UnsupportedEncodingException 编码异常
     */
    private void setDownloadHeader(HttpServletResponse response, String fileName, String contentType) throws UnsupportedEncodingException {
        String encoded = URLEncoder.encode(fileName, Constants.CONTROL_ENCODING).replace("+", "%20");
        response.reset();
        response.setCharacterEncoding(Constants.CONTROL_ENCODING);
        response.setContentType(contentType);
        response.setHeader(Constants.HEADER_CONFIG.CONTENT_DISPOSITION, "attachment; filename*=UTF-8''" + encoded);
        response.setHeader(Constants.HEADER_CONFIG.CACHE_CONTROL, "no-cache, no-store, must-revalidate");
        response.setHeader(Constants.HEADER_CONFIG.PRAGMA, "no-cache");
        response.setDateHeader(Constants.HEADER_CONFIG.EXPIRES, 0);
    }
}