package com.pufferfishscheduler.service.database.resource.service.impl;

import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.dict.service.DictService;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.domain.vo.TreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceTreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.service.database.db.connector.ftp.FTPManager;
import com.pufferfishscheduler.service.database.db.connector.ftp.FTPSManager;
import com.pufferfishscheduler.service.database.db.service.DbBasicService;
import com.pufferfishscheduler.service.database.db.service.DbDatabaseService;
import com.pufferfishscheduler.service.database.db.service.DbGroupService;
import com.pufferfishscheduler.service.database.resource.service.ResourceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 资源 Service实现类
 */
@Slf4j
@Service
public class ResourceServiceImpl implements ResourceService {

    @Autowired
    private AESUtil aesUtil;

    @Value("${ftp.path}")
    private String ftpPath;

    @Autowired
    private DictService dictService;

    @Autowired
    private DbGroupService dbGroupService;

    @Autowired
    private DbBasicService dbBasicService;

    @Autowired
    private DbDatabaseService databaseService;

    /**
     * 分页获取资源文件列表
     *
     * @param dbId
     * @param name
     * @param path
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public IPage<ResourceVo> list(Integer dbId, String name, String path, Integer pageNo, Integer pageSize) {
        IPage<ResourceVo> page = new Page<>(pageNo, pageSize);

        if (dbId == null) return page;

        DbDatabase database = getDbDatabase(dbId);
        List<ResourceVo> resultList;

        try {
            // 指定FTP操作区域
            path = ftpPath + Constants.FILE_SEPARATOR + handleBuiltInDirectory(path);

            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                resultList = handleFtpList(database, path, name);
            } else if (Constants.FTP_TYPE.FTPS.equals(database.getType())) {
                resultList = handleFtpsList(database, path, name);
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

        // 排序
        pageList.sort(Comparator.comparing(ResourceVo::getCreatedTime));

        page.setTotal(total);
        page.setRecords(pageList);
        return page;
    }

    /**
     * 获取资源目录列表(树形结构)
     *
     * @param dbId
     * @return
     */
    @Override
    public List<ResourceTreeVo> directoryTree(Integer dbId, String path) {

        // 参数校验
        if (dbId == null) {
            throw new BusinessException("数据源ID不能为空");
        }
        if (StringUtils.isBlank(path)) {
            throw new BusinessException("路径参数不能为空");
        }

        // 获取数据源
        DbDatabase database = getDbDatabase(dbId);
        FTPManager ftpManager = null;
        FTPSManager ftpsManager = null;
        FTPClient ftpClient = null;

        try {
            // 根据类型创建连接
            if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
                ftpManager = getFtpManager(database);
                ftpClient = ftpManager.getFTPClient();
            } else {
                ftpsManager = getFtpsManager(database);
                ftpClient = ftpsManager.getFTPClient();
            }

            // 标准化路径（确保以/结尾）
            String normalizedPath = normalizePath(path);
            // 指定FTP操作区域
//            normalizedPath = ftpPath + Constants.FILE_SEPARATOR + handleBuiltInDirectory(normalizedPath);
            normalizedPath = ftpPath;

            // 验证路径是否存在
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
     * 递归列出指定路径下的目录结构
     *
     * @param ftpClient
     * @param basePath
     * @return
     * @throws IOException
     */
    private List<ResourceTreeVo> listDirectories(FTPClient ftpClient, String basePath) throws IOException {
        List<ResourceTreeVo> resultList = new ArrayList<>();

        // 列出当前目录内容
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

            // 递归获取子目录
            List<ResourceTreeVo> children = listDirectories(ftpClient, childPath);
            if (!children.isEmpty()) {
                vo.setChildren(children);
            }

            resultList.add(vo);
        }
        return resultList;
    }

    /**
     * 验证是否为有效目录
     *
     * @param file
     * @return
     */
    private boolean isValidDirectory(FTPFile file) {
        return file != null &&
                file.isDirectory() &&
                !".".equals(file.getName()) &&
                !"..".equals(file.getName());
    }

    /**
     * 构建子目录完整路径
     *
     * @param basePath
     * @param dirName
     * @return
     */
    private String buildChildPath(String basePath, String dirName) {
        if (basePath.endsWith(Constants.FILE_SEPARATOR)) {
            return basePath + dirName;
        }
        return basePath + Constants.FILE_SEPARATOR + dirName;
    }

    /**
     * 标准化路径格式
     *
     * @param path
     * @return
     */
    private String normalizePath(String path) {
        // 确保路径以/开头
        if (!path.startsWith(Constants.FILE_SEPARATOR)) {
            path = Constants.FILE_SEPARATOR + path;
        }

        // 确保路径不以/结尾（除非是根目录）
        if (!Constants.FILE_SEPARATOR.equals(path) && path.endsWith(Constants.FILE_SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * 安全关闭资源
     *
     * @param ftpManager
     * @param ftpsManager
     */
    private void closeResources(FTPManager ftpManager, FTPSManager ftpsManager) {
        try {
            if (ftpManager != null) {
                ftpManager.close();
            }
            if (ftpsManager != null) {
                ftpsManager.close();
            }
        } catch (Exception e) {
            log.warn("关闭FTP资源时出错: {}", e.getMessage());
        }
    }

    /**
     * 上传文件
     *
     * @param dbId
     * @param path
     * @param files
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

        // 指定FTP操作区域
        path = ftpPath + Constants.FILE_SEPARATOR + handleBuiltInDirectory(path);

        DbDatabase database = getDbDatabase(dbId);
        // 创建目录
        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpUpload(database, path, files);
        } else {
            ftpsUpload(database, path, files);
        }

    }

    /**
     * 创建目录
     *
     * @param form
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
        // 指定操作区域
        String remotePath = ftpPath + handleBuiltInDirectory(form.getRemotePath());

        // 获取FTP/FTPS数据源
        DbDatabase database = getDbDatabase(form.getDbId());

        // 创建目录
        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpMkdir(database, remotePath, name);
        } else {
            ftpsMkdir(database, remotePath, name);
        }

    }

    /**
     * 重命名
     *
     * @param form
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

        // 指定FTP操作区域
        String path = ftpPath + Constants.FILE_SEPARATOR + handleBuiltInDirectory(form.getPath());

        DbDatabase database = getDbDatabase(form.getDbId());
        // 创建目录
        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpRename(database, path, oldName, newName);
        } else {
            ftpsRename(database, path, oldName, newName);
        }
    }

    /**
     * 移动
     *
     * @param form
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

        // 指定FTP操作区域
        fromPath = ftpPath + fromPath;
        toPath = ftpPath + toPath;

        DbDatabase database = getDbDatabase(form.getDbId());
        // 创建目录
        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpMove(database, form.getType(), fromPath, toPath);
        } else {
            ftpsMove(database, form.getType(), fromPath, toPath);
        }
    }

    /**
     * 移除
     *
     * @param dbId
     * @param type
     * @param path
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

        // 指定FTP操作区域
        String remotePath = ftpPath + Constants.FILE_SEPARATOR + handleBuiltInDirectory(path);

        DbDatabase database = getDbDatabase(dbId);

        // 创建目录
        if (Constants.FTP_TYPE.FTP.equals(database.getType())) {
            ftpRemove(database, type, remotePath);
        } else {
            ftpsRemove(database, type, remotePath);
        }
    }

    /**
     * 构建path
     *
     * @param path
     * @param name
     * @return
     */
    private String buildPath(String path, String name) {
        if (StringUtils.isBlank(name)) {
            if (path.endsWith(Constants.FILE_SEPARATOR) && !path.equals(Constants.FILE_SEPARATOR)) {
                return path.substring(0, path.length() - 1);
            }
            return path;
        }

        if (path.equals(Constants.FILE_SEPARATOR)) {
            return name.startsWith(Constants.FILE_SEPARATOR) ? name : Constants.FILE_SEPARATOR + name;
        }

        boolean pathEndsWithSep = path.endsWith(Constants.FILE_SEPARATOR);
        boolean nameStartsWithSep = name.startsWith(Constants.FILE_SEPARATOR);

        if (pathEndsWithSep && nameStartsWithSep) {
            return path + name.substring(1);
        } else if (!pathEndsWithSep && !nameStartsWithSep) {
            return path + Constants.FILE_SEPARATOR + name;
        } else {
            return path + name;
        }
    }

    /**
     * 处理内置目录
     * @param path
     * @return
     */
    private String handleBuiltInDirectory(String path) {
        return path.startsWith(ftpPath) ? dealPath(path.replace(ftpPath,"")) : dealPath(path);
    }

    /**
     * 处理path路径
     *
     * @param path
     * @return
     */
    private String dealPath(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }

        if (path.startsWith(Constants.FILE_SEPARATOR)) {
            return path;
        }

        return Constants.FILE_SEPARATOR + path;
    }


    /**
     * 处理时区
     */
    private void setTimeZone() {
        TimeZone time = TimeZone.getTimeZone(Constants.TIME_ZONE);
        TimeZone.setDefault(time);
    }

    /**
     * 处理FTP文件集合
     *
     * @param database
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    private List<ResourceVo> handleFtpList(DbDatabase database, String path, String name) throws IOException {
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {
            if (!ftpClient.changeWorkingDirectory(path)) {
                return Collections.emptyList();
            }
            FTPFile[] fs = ftpClient.listFiles();
            return ArrayUtils.isNotEmpty(fs) ? buildList(fs, name, database.getId()) : Collections.emptyList();
        } finally {
            ftpManager.close();
        }
    }

    /**
     * 处理FTPS文件集合
     *
     * @param database
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    private List<ResourceVo> handleFtpsList(DbDatabase database, String path, String name) throws IOException {
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPSClient ftpsClient = ftpsManager.getFTPClient();
        try {
            if (!ftpsClient.changeWorkingDirectory(path)) {
                return Collections.emptyList();
            }
            FTPFile[] fs = ftpsClient.listFiles();
            return ArrayUtils.isNotEmpty(fs) ? buildList(fs, name, database.getId()) : Collections.emptyList();
        } finally {
            ftpsManager.close();
        }
    }

    /**
     * 构建文件集合
     *
     * @param files
     * @param nameFilter
     * @param dbId
     * @return
     */
    private List<ResourceVo> buildList(FTPFile[] files, String nameFilter, Integer dbId) {
        List<ResourceVo> resultList = new ArrayList<>();
        for (FTPFile file : files) {
            String fileName = file.getName();

            if (file.isDirectory()) {
                if (".".equals(fileName) || "..".equals(fileName)) continue;
            }

            if (StringUtils.isNotBlank(nameFilter) && !fileName.contains(nameFilter)) {
                continue;
            }

            ResourceVo vo = new ResourceVo();
            vo.setName(fileName);
            vo.setType(file.isDirectory() ? Constants.FTP_FILE_TYPE.DIRECTORY : Constants.FTP_FILE_TYPE.FILE);
            vo.setCreatedTime(Date.from(
                    file.getTimestamp().toInstant()
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
            ));
            if (file.isFile()) {
                vo.setSize(formatBytes(file.getSize()));
            } else {
                vo.setSize(Constants.DIRECTORY_SIZE);
            }
            vo.setDbId(dbId);

            resultList.add(vo);
        }
        return resultList;
    }


    /**
     * 转换字节为KB
     *
     * @param bytes 字节大小
     * @return MB
     */
    private String formatBytes(long bytes) {
        if (bytes <= 0) return "0KB";

        double kb = bytes / 1024.0;
        if (kb < 0.01) return "<0.01KB";

        return String.format("%.2fKB", kb);
    }

    /**
     * 获取FTP管理器
     *
     * @param database
     * @return
     */
    private FTPManager getFtpManager(DbDatabase database) {

        FTPManager ftpManager = new FTPManager();
        ftpManager.setHost(database.getDbHost());
        ftpManager.setPort(Integer.parseInt(database.getDbPort()));
        ftpManager.setUser(database.getUsername());
        // 解密
        String pwd = aesUtil.decrypt(database.getPassword());
        ftpManager.setPassword(pwd);
        // 解析配置信息
        if (StringUtils.isNotBlank(database.getProperties())) {
            JSONObject properties = JSONObject.parseObject(database.getProperties());

            ftpManager.setMode(properties.containsKey(Constants.FTP_PROPERTIES.MODE) ?
                    properties.getString(Constants.FTP_PROPERTIES.MODE) : Constants.MODE_TYPE.PASSIVE);
            ftpManager.setControlEncoding(properties.containsKey(Constants.FTP_PROPERTIES.CONTROL_ENCODING) ?
                    dictService.getDictItemCode(Constants.DICT.CONTROL_ENCODING, properties.getString(Constants.FTP_PROPERTIES.CONTROL_ENCODING))
                    : Constants.CONTROL_ENCODING);
        }

        return ftpManager;
    }

    /**
     * 获取FTPS管理器
     *
     * @param database
     * @return
     */
    private FTPSManager getFtpsManager(DbDatabase database) {

        FTPSManager ftpsManager = new FTPSManager();
        ftpsManager.setHost(database.getDbHost());
        ftpsManager.setPort(Integer.parseInt(database.getDbPort()));
        ftpsManager.setUser(database.getUsername());
        // 解密
        String pwd = aesUtil.decrypt(database.getPassword());
        ftpsManager.setPassword(pwd);
        // 解析配置信息
        if (StringUtils.isNotBlank(database.getProperties())) {
            JSONObject properties = JSONObject.parseObject(database.getProperties());

            ftpsManager.setMode(properties.containsKey(Constants.FTP_PROPERTIES.MODE) ?
                    properties.getString(Constants.FTP_PROPERTIES.MODE) : Constants.MODE_TYPE.PASSIVE);
            ftpsManager.setControlEncoding(properties.containsKey(Constants.FTP_PROPERTIES.CONTROL_ENCODING) ?
                    properties.getString(Constants.FTP_PROPERTIES.CONTROL_ENCODING) : Constants.CONTROL_ENCODING);
        }

        return ftpsManager;
    }

    /**
     * FTP上传文件
     *
     * @param database
     * @param path
     * @param files
     */
    private void ftpUpload(DbDatabase database, String path, List<MultipartFile> files) {
        FTPManager ftpManager = getFtpManager(database);
        try {

            for (MultipartFile file : files) {

                String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                Date currentTime = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                String date = formatter.format(currentTime);
                String pathName = ftpPath + File.separator + date + File.separator + uuid;
                File dir = new File(pathName);
                if (!dir.exists()) {
                    boolean mkdirs = dir.mkdirs();
                    if (!mkdirs) {
                        log.error("创建临时文件夹失败");
                        throw new BusinessException("创建临时文件夹失败");
                    }
                }

                String filePath = pathName + Constants.FILE_SEPARATOR + file.getOriginalFilename();
                File file1 = new File(filePath);
                FileUtils.copyInputStreamToFile(file.getInputStream(), file1);
                ftpManager.init();
                boolean result = ftpManager.uploadFile(path, filePath, false);
                if (result) {
                    log.info(String.format("上传文件【%s】到【%s】成功！", file.getOriginalFilename(), path));
                } else {
                    log.info(String.format("上传文件【%s】到【%s】失败！", file.getOriginalFilename(), path));
                }

                // 删除本地临时文件夹
                deleteFolder(pathName);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException("上传文件失败！");
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    /**
     * FTPS上传文件
     *
     * @param database
     * @param path
     * @param files
     */
    private void ftpsUpload(DbDatabase database, String path, List<MultipartFile> files) {
        FTPSManager ftpsManager = getFtpsManager(database);
        try {

            for (MultipartFile file : files) {

                String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                Date currentTime = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                String date = formatter.format(currentTime);
                String pathName = ftpPath + File.separator + date + File.separator + uuid;
                File dir = new File(pathName);
                if (!dir.exists()) {
                    boolean mkdirs = dir.mkdirs();
                    if (!mkdirs) {
                        log.error("创建临时文件夹失败");
                        throw new BusinessException("创建临时文件夹失败");
                    }
                }

                String filePath = pathName + Constants.FILE_SEPARATOR + file.getOriginalFilename();
                File file1 = new File(filePath);
                FileUtils.copyInputStreamToFile(file.getInputStream(), file1);
                ftpsManager.init();
                boolean result = ftpsManager.uploadFile(path, filePath, false);
                if (result) {
                    log.info(String.format("上传文件【%s】到【%s】成功！", file.getOriginalFilename(), path));
                } else {
                    log.info(String.format("上传文件【%s】到【%s】失败！", file.getOriginalFilename(), path));
                }

                // 删除本地临时文件夹
                deleteFolder(pathName);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException("上传文件失败！");
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }


    /**
     * FTP创建目录
     *
     * @param database
     * @param remotePath
     * @param name
     */
    private void ftpMkdir(DbDatabase database, String remotePath, String name) {
        // 转换为中国时区
        setTimeZone();

        FTPManager ftpManager = getFtpManager(database);
        try {
            ftpManager.init();
            FTPClient ftpClient = ftpManager.getFTPClient();

            String path = buildPath(remotePath, name);

            ftpClient.changeWorkingDirectory(path);
            int replyCode = ftpClient.getReplyCode();
            if (replyCode != 550) {
                throw new BusinessException();
            }
            ftpClient.changeWorkingDirectory(remotePath);
            ftpManager.createRemoteDirectory(path);
            log.info(String.format("创建【%s】目录成功！", name));
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException("创建目录失败！");
        } finally {

            try {
                ftpManager.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    /**
     * FTPS创建目录
     *
     * @param database
     * @param remotePath
     * @param name
     */
    private void ftpsMkdir(DbDatabase database, String remotePath, String name) {

        // 转换为中国时区
        setTimeZone();

        FTPSManager ftpsManager = getFtpsManager(database);
        try {
            ftpsManager.init();
            FTPSClient ftpsClient = ftpsManager.getFTPClient();

            String path = buildPath(remotePath, name);

            ftpsClient.changeWorkingDirectory(path);
            int replyCode = ftpsClient.getReplyCode();
            if (replyCode != 550) {
                throw new BusinessException();
            }
            ftpsClient.changeWorkingDirectory(remotePath);
            ftpsManager.createRemoteDirectory(path);
            log.info(String.format("创建【%s】目录成功！", name));
        } catch (Exception e) {
            if (e.getClass().equals(BusinessException.class)) {
                throw new BusinessException("目录已存在，创建目录失败！");
            }
            log.error(e.getMessage());
            throw new BusinessException("创建目录失败！");
        } finally {

            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    /**
     * FTP重命名
     *
     * @param database
     * @param path
     * @param oldName
     * @param newName
     * @return
     */
    private void ftpRename(DbDatabase database, String path, String oldName, String newName) {

        // 转换为中国时区
        setTimeZone();

        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();
        try {

            String oldPath = buildPath(path, oldName);
            String newPath = buildPath(path, newName);

            boolean result = ftpClient.rename(oldPath, newPath);
            if (!result) {
                throw new BusinessException(String.format("重命名【%s】失败", newName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(String.format("重命名【%s】失败", newName));
        } finally {

            try {
                ftpManager.close();
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }
    }

    /**
     * FTPS重命名
     *
     * @param database
     * @param path
     * @param oldName
     * @param newName
     * @return
     */
    private void ftpsRename(DbDatabase database, String path, String oldName, String newName) {

        // 转换为中国时区
        setTimeZone();

        FTPSManager ftpsManager = getFtpsManager(database);
        FTPSClient ftpsClient = ftpsManager.getFTPClient();
        try {

            String oldPath = buildPath(path, oldName);
            String newPath = buildPath(path, newName);

            boolean result = ftpsClient.rename(oldPath, newPath);
            if (!result) {
                throw new BusinessException(String.format("重命名【%s】失败", newName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(String.format("重命名【%s】失败", newName));
        } finally {

            try {
                ftpsManager.close();
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }
    }

    /**
     * FTP移动文件
     *
     * @param database
     * @param type
     * @param fromPath
     * @param toPath
     */
    private void ftpMove(DbDatabase database, String type, String fromPath, String toPath) {

        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();

        try {
            // 规范化路径（确保不以/结尾）
            toPath = toPath.endsWith(Constants.FILE_SEPARATOR) ? toPath.substring(0, toPath.length() - 1) : toPath;

            if (type.equals(Constants.FTP_FILE_TYPE.DIRECTORY)) {
                // 目录移动逻辑
                if (ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录已存在同名目录: " + toPath);
                }

                // 检查根目录一致性
                String fromRoot = extractRootDirectory(fromPath);
                String toRoot = extractRootDirectory(toPath);
                if (!fromRoot.equals(toRoot)) {
                    throw new BusinessException("不能跨根目录移动: " + fromRoot + " -> " + toRoot);
                }

            } else { // 文件移动逻辑
                // 检查目标目录是否存在
                if (!ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录不存在: " + toPath);
                }

                // 构造完整目标路径
                String fileName = fromPath.contains(Constants.FILE_SEPARATOR)
                        ? fromPath.substring(fromPath.lastIndexOf('/') + 1)
                        : fromPath;
                toPath += Constants.FILE_SEPARATOR + fileName;
            }
            // 执行移动操作
            boolean result = ftpClient.rename(fromPath, toPath);
            if (!result) {
                int replyCode = ftpClient.getReplyCode();
                String replyString = ftpClient.getReplyString();
                throw new BusinessException(String.format(
                        "移动失败 [%d]: %s -> %s | %s",
                        replyCode, fromPath, toPath, replyString
                ));
            }
        } catch (Exception e) {
            log.error("FTP操作异常: {}", e.getMessage(), e);
            throw new BusinessException(String.format("%s移动%s失败", fromPath, toPath));
        } finally {
            try {
                ftpManager.close();
            } catch (Exception e) {
                log.warn("FTP连接关闭异常: {}", e.getMessage());
            }
        }
    }

    /**
     * FTPS移动文件
     *
     * @param database
     * @param type
     * @param fromPath
     * @param toPath
     */
    private void ftpsMove(DbDatabase database, String type, String fromPath, String toPath) {

        FTPSManager ftpsManager = getFtpsManager(database);
        FTPClient ftpClient = ftpsManager.getFTPClient();

        try {
            // 规范化路径（确保不以/结尾）
            toPath = toPath.endsWith(Constants.FILE_SEPARATOR) ? toPath.substring(0, toPath.length() - 1) : toPath;

            if (type.equals(Constants.FTP_FILE_TYPE.DIRECTORY)) {
                // 目录移动逻辑
                if (ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录已存在同名目录: " + toPath);
                }

                // 检查根目录一致性
                String fromRoot = extractRootDirectory(fromPath);
                String toRoot = extractRootDirectory(toPath);
                if (!fromRoot.equals(toRoot)) {
                    throw new BusinessException("不能跨根目录移动: " + fromRoot + " -> " + toRoot);
                }

            } else {
                // 文件移动逻辑
                // 检查目标目录是否存在
                if (!ftpClient.changeWorkingDirectory(toPath)) {
                    throw new BusinessException("目标目录不存在: " + toPath);
                }

                // 构造完整目标路径
                String fileName = fromPath.contains(Constants.FILE_SEPARATOR)
                        ? fromPath.substring(fromPath.lastIndexOf('/') + 1)
                        : fromPath;
                toPath += Constants.FILE_SEPARATOR + fileName;
            }
            // 执行移动操作
            boolean result = ftpClient.rename(fromPath, toPath);
            if (!result) {
                int replyCode = ftpClient.getReplyCode();
                String replyString = ftpClient.getReplyString();
                throw new BusinessException(String.format(
                        "移动失败 [%d]: %s -> %s | %s",
                        replyCode, fromPath, toPath, replyString
                ));
            }
        } catch (Exception e) {
            log.error("FTPS操作异常: {}", e.getMessage(), e);
            throw new BusinessException(String.format("%s移动%s失败", fromPath, toPath));
        } finally {
            try {
                ftpsManager.close();
            } catch (Exception e) {
                log.warn("FTPS连接关闭异常: {}", e.getMessage());
            }
        }
    }

    /**
     * 辅助根目录
     *
     * @param path
     * @return
     */
    private String extractRootDirectory(String path) {
        if (path == null || path.isEmpty()) return Constants.FILE_SEPARATOR;
        String[] parts = path.split(Constants.FILE_SEPARATOR);
        return parts.length > 1 ? Constants.FILE_SEPARATOR + parts[1] : Constants.FILE_SEPARATOR;
    }

    /**
     * FTP移除文件
     *
     * @param database
     * @param type
     * @param path
     */
    private void ftpRemove(DbDatabase database, String type, String path) {
        FTPManager ftpManager = getFtpManager(database);
        FTPClient ftpClient = ftpManager.getFTPClient();

        boolean result = false;

        try {

            if (Constants.FTP_FILE_TYPE.FILE.equals(type)) {
                result = ftpClient.deleteFile(path);
            }
            if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                deleteFolder(path, ftpClient);
                result = ftpClient.removeDirectory(path);
            }

            if (!result) {
                log.debug(String.format("【%s】删除失败！", path));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(String.format("【%s】删除失败", path));
        } finally {
            ftpManager.close();
        }
    }

    /**
     * FTPS移除文件
     *
     * @param database
     * @param type
     * @param path
     */
    private void ftpsRemove(DbDatabase database, String type, String path) {
        FTPSManager ftpsManager = getFtpsManager(database);
        FTPClient ftpClient = ftpsManager.getFTPClient();

        boolean result = false;

        try {

            if (Constants.FTP_FILE_TYPE.FILE.equals(type)) {
                result = ftpClient.deleteFile(path);
            }
            if (Constants.FTP_FILE_TYPE.DIRECTORY.equals(type)) {
                deleteFolder(path, ftpClient);
                result = ftpClient.removeDirectory(path);
            }

            if (!result) {
                log.debug(String.format("【%s】删除失败！", path));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(String.format("【%s】删除失败", path));
        } finally {
            ftpsManager.close();
        }
    }

    /**
     * 递归删除FTP文件夹
     *
     * @param dirPath
     * @param ftpClient
     */
    public void deleteFolder(String dirPath, FTPClient ftpClient) {

        try {
            ftpClient.changeWorkingDirectory(dirPath);
            FTPFile[] ftpFiles = ftpClient.listFiles();
            if (ArrayUtils.isNotEmpty(ftpFiles)) {
                for (FTPFile ftpFile : ftpFiles) {
                    String filePath = dirPath + dealPath(ftpFile.getName());
                    if (ftpFile.isFile()) {
                        ftpClient.deleteFile(filePath);
                    } else {
                        deleteFolder(filePath, ftpClient);
                    }
                    ftpClient.removeDirectory(filePath);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    /**
     * 删除本地文件夹
     *
     * @param path
     */
    public void deleteFolder(String path) {

        File folder = new File(path);
        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (ArrayUtils.isNotEmpty(files)) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteFolder(path + dealPath(file.getName()));
                    } else {
                        file.delete();//删除文件夹下文件
                    }
                }
            }
            folder.delete();//删除空文件夹
        }
    }

    /**
     * 数据源结构
     *
     * @param name
     * @return
     */
    @Override
    public List<TreeVo> tree(String name) {

        // 查询分组
        List<DbGroup> dbGroups = dbGroupService.getGroupList();
        if (dbGroups.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Integer, TreeVo> groupMap = dbGroups.stream()
                .collect(Collectors.toMap(DbGroup::getId, this::createGroupVo));

        // 查询FTP数据源
        List<DbDatabase> databaseList = databaseService.listFTPDatabaseList(name);
        if (databaseList.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Integer, List<TreeVo>> dbMap = databaseList.stream()
                .collect(Collectors.groupingBy(DbDatabase::getGroupId,
                        Collectors.mapping(this::createDbDatabase, Collectors.toList())));

        // 存储结果
        List<TreeVo> result = new LinkedList<>();

        // 构建树形结构
        for (DbGroup group : dbGroups) {
            TreeVo current = groupMap.get(group.getId());

            // 添加记录数据源子节点
            List<TreeVo> children = dbMap.get(group.getId());
            if (children != null) {
                current.getChildren().addAll(children);
            }

            // 直接加入结果集
            result.add(current);
        }

        // 过滤空数组
        result = filterEmptyGroups(result);

        // 排序
        sort(result);

        return result;
    }

    /**
     * 对result列表进行过滤
     *
     * @param nodes
     * @return
     */
    private List<TreeVo> filterEmptyGroups(List<TreeVo> nodes) {
        List<TreeVo> filtered = new ArrayList<>();

        for (TreeVo node : nodes) {
            // 先过滤子节点
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                node.setChildren(filterEmptyGroups(node.getChildren()));
            }

            // 判断当前节点是否需要保留
            if (!shouldFilterOut(node)) {
                filtered.add(node);
            }
        }

        return filtered;
    }

    /**
     * 判断是否需要过滤掉该节点
     *
     * @param node
     * @return
     */
    private boolean shouldFilterOut(TreeVo node) {
        // 非分组节点不过滤
        if (!Constants.TREE_TYPE.GROUP.equals(node.getType())) {
            return false;
        }

        // 没有子节点的分组过滤掉
        if (node.getChildren().isEmpty()) {
            return true;
        }

        // 检查是否所有子节点都是空分组
        boolean allChildrenAreEmptyGroups = true;
        for (TreeVo child : node.getChildren()) {
            if (!Constants.TREE_TYPE.GROUP.equals(child.getType()) || !child.getChildren().isEmpty()) {
                allChildrenAreEmptyGroups = false;
                break;
            }
        }

        return allChildrenAreEmptyGroups;
    }

    /**
     * 排序
     *
     * @param result
     */
    public static void sort(List<TreeVo> result) {
        for (TreeVo treeVo : result) {
            if (Constants.TREE_TYPE.GROUP.equals(treeVo.getType()) &&
                    !treeVo.getChildren().isEmpty()) {
                treeVo.getChildren().sort(Comparator.comparingInt(TreeVo::getOrderBy));
            }
        }
    }

    /**
     * 封装分类
     *
     * @param group
     * @return
     */
    private TreeVo createGroupVo(DbGroup group) {
        TreeVo groupTree = new TreeVo();
        groupTree.setId(group.getId());
        groupTree.setName(group.getName());
        groupTree.setType(Constants.TREE_TYPE.GROUP);
        groupTree.setChildren(new ArrayList<>());
        groupTree.setOrderBy(group.getOrderBy());
        return groupTree;
    }

    /**
     * 封装数据源
     *
     * @param database
     * @return
     */
    private TreeVo createDbDatabase(DbDatabase database) {
        TreeVo databaseTree = new TreeVo();
        databaseTree.setId(database.getId());
        databaseTree.setName(database.getName());
        databaseTree.setParentId(database.getGroupId());
        databaseTree.setType(Constants.TREE_TYPE.DATABASE);
        databaseTree.setTreeParentId(Objects.isNull(database.getGroupId()) ? null : Constants.TREE_TYPE.GROUP + database.getGroupId());
        databaseTree.setChildren(new ArrayList<>());
        databaseTree.setTreeId(Constants.TREE_TYPE.DATABASE + database.getId());
        databaseTree.setIcon(dbBasicService.getDbIcon(database.getType()));
        databaseTree.setOrderBy(database.getId());
        return databaseTree;
    }

    /**
     * 获取FTP数据源
     *
     * @param dbId
     * @return
     */
    private DbDatabase getDbDatabase(Integer dbId) {

        List<String> type = Arrays.asList(Constants.FTP_TYPE.FTP, Constants.FTP_TYPE.FTPS);

        DbDatabase dbDatabase = databaseService.getDatabaseById(dbId);
        if (Objects.isNull(dbDatabase)) {
            throw new BusinessException("请校验数据源是否存在!");
        }

        if (!type.contains(dbDatabase.getType())) {
            throw new BusinessException(String.format("请校验数据源【%s】的类型是否是FTP/FTPS!", dbDatabase.getName()));
        }

        return dbDatabase;
    }
}
