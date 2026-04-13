package com.pufferfishscheduler.master.database.resource.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.domain.vo.database.ResourceTreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Set;

/**
 * 资源管理 Service
 */
public interface ResourceService {

    /**
     * 分页获取资源列表
     *
     * @param dbId     数据源Id
     * @param name     资源名称
     * @param path     资源路径
     * @param pageNo   页码
     * @param pageSize 每页数量
     * @return
     */
    IPage<ResourceVo> list(Integer dbId, String name, String path, Integer pageNo, Integer pageSize);

    /**
     * 获取资源列表
     *
     * @param dbId            数据源Id
     * @param path            资源路径
     * @param fileTypeSet     文件类型集合
     * @return
     */
    List<ResourceVo> getResourceList(Integer dbId, String path, List<String> fileTypeSet);

    /**
     * 上传文件
     *
     * @param dbId  数据源Id
     * @param path  上传目录
     * @param files 文件列表
     */
    void upload(Integer dbId, String path, List<MultipartFile> files);

    /**
     * 上传文件并指定文件名
     *
     * @param dbId      数据源Id
     * @param path      上传目录
     * @param fileNames 文件名列表
     */
    void uploadWithNames(Integer dbId, String path, List<String> fileNames);

    /**
     * 创建文件夹
     *
     * @param form
     */
    void mkdir(ResourceForm form);

    /**
     * 重命名
     *
     * @param form
     */
    void rename(ResourceForm form);

    /**
     * 移动
     *
     * @param form
     */
    void move(ResourceForm form);

    /**
     * 移除
     *
     * @param dbId
     * @param type
     * @param path
     */
    void remove(Integer dbId, String type, String path);

    /**
     * 下载zip包
     *
     * @param response 响应对象
     * @param form     下载表单
     */
    void download(HttpServletResponse response, ResourceForm form);

    /**
     * 获取资源目录列表
     *
     * @param dbId 数据源Id
     * @param path 资源路径
     * @return
     */
    List<ResourceTreeVo> directoryTree(Integer dbId, String path);

    /**
     * 删除本地临时目录及其所有内容
     *
     * @param localPath 本地目录绝对路径
     */
    void deleteLocalDirectory(String localPath);

    /**
     * 从 FTP/FTPS 下载单个文件到本地（本地父目录不存在时会创建）
     *
     * @param dbId               数据源 ID
     * @param remoteRelativeDir  远程目录（与资源浏览中的相对路径一致；为空表示仅使用配置的 FTP 根路径）
     * @param fileName           远程文件名
     * @param localFullPath      本地保存完整路径（含文件名）
     */
    default void downloadFileToLocal(Integer dbId, String remoteRelativeDir, String fileName, String localFullPath) {
        downloadFileToLocal(dbId, remoteRelativeDir, fileName, localFullPath, false);
    }

    /**
     * 同 {@link #downloadFileToLocal(Integer, String, String, String)}；
     * 当 {@code trustLocalCacheWithoutFtp} 为 true 且本地已存在有效 Excel 缓存时，不再连接 FTP（用于设计器连续调用 getSheets/getFields）。
     */
    void downloadFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCacheWithoutFtp);
}
