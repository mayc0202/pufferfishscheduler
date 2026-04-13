package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.DatabaseCategory;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.vo.collect.FileVo;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import com.pufferfishscheduler.master.collect.trans.plugin.service.FtpPluginService;
import com.pufferfishscheduler.master.database.database.service.DbGroupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class FtpPluginServiceImpl implements FtpPluginService {

    @Autowired
    private FilePathConfig filePathConfig;

    @Autowired
    private DbGroupService dbGroupService;

    /**
     * 获取FTP数据源
     *
     * @return
     */
    @Override
    public List<Tree> ftpDbTree() {
        return dbGroupService.dbTreeByCategory(DatabaseCategory.FTP);
    }

    /**
     * 获取本地目录下的文件
     *
     * @param path 本地目录路径
     * @param type 文件类型 1-代表获取所有的目录和文件；2-代表只获取目录；3-获取zip文件
     * @return 文件列表
     */
    @Override
    public List<FileVo> getLocalDirectory(String path, String type) {
        // 1. 构建完整路径
        String fullPath = filePathConfig.getLocalPath() + File.separator + path;
        File rootFile = new File(fullPath);

        // 2. 安全校验
        if (!rootFile.exists()) {
            log.warn("目录不存在：{}", fullPath);
            return new ArrayList<>();
        }
        if (!rootFile.isDirectory()) {
            log.error("不是有效目录：{}", fullPath);
            throw new BusinessException("路径不是有效目录");
        }

        // 3. 列出文件
        File[] files = rootFile.listFiles();
        if (ArrayUtils.isEmpty(files)) {
            return new ArrayList<>();
        }

        List<FileVo> resultList = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                boolean isDir = file.isDirectory();
                boolean isFile = file.isFile();

                // ==================== 类型筛选 ====================
                boolean needAdd = switch (type) {
                    case Constants.LOCAL_FILE_TYPE.ALL -> true;
                    case Constants.LOCAL_FILE_TYPE.DIR -> isDir;
                    case Constants.LOCAL_FILE_TYPE.ZIP -> isDir || (isFile && file.getName().endsWith(".zip"));
                    default -> false;
                };

                if (!needAdd) {
                    continue;
                }

                // ==================== 构建VO ====================
                FileVo fileVo = new FileVo();
                fileVo.setName(file.getName());
                fileVo.setPath(file.getPath().replace(filePathConfig.getLocalPath(), ""));
                fileVo.setType(isDir ? Constants.FTP_FILE_TYPE.DIRECTORY : Constants.FTP_FILE_TYPE.FILE);
                resultList.add(fileVo);
            }
        }

        return resultList;
    }

}
