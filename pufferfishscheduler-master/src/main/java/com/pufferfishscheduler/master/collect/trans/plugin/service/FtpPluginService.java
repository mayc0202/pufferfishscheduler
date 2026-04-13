package com.pufferfishscheduler.master.collect.trans.plugin.service;

import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.vo.collect.FileVo;

import java.util.List;

public interface FtpPluginService {

    /**
     * 获取ftp数据源
     *
     * @return
     */
    List<Tree> ftpDbTree();

    /**
     * 获取本地目录下的文件
     * @param path 本地目录路径
     * @param type 文件类型
     * @return 文件列表
     */
    List<FileVo> getLocalDirectory(String path, String type);
}
