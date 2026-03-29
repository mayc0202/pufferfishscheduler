package com.pufferfishscheduler.ai.knowledge.storage;

import com.pufferfishscheduler.ai.knowledge.config.KnowledgeFtpProperties;
import com.pufferfishscheduler.common.exception.BusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * 基于FTP的知识库附件存储
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FtpKnowledgeFileStorage implements KnowledgeFileStorage {

    private final KnowledgeFtpProperties ftpProperties;

    @Override
    public String upload(MultipartFile file, String relativeDir) {
        FTPClient ftpClient = new FTPClient();
        String fileName = buildFileName(file.getOriginalFilename());
        String remoteDir = normalizePath(ftpProperties.getBasePath()) + normalizePath(relativeDir);
        String remotePath = remoteDir + "/" + fileName;
        try {
            connect(ftpClient);
            ensureDirectory(ftpClient, remoteDir);
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            try (InputStream inputStream = file.getInputStream()) {
                boolean uploaded = ftpClient.storeFile(remotePath, inputStream);
                if (!uploaded) {
                    throw new BusinessException("上传FTP附件失败: " + ftpClient.getReplyString());
                }
            }
            return remotePath;
        } catch (IOException e) {
            throw new BusinessException("上传FTP附件失败: " + e.getMessage());
        } finally {
            disconnect(ftpClient);
        }
    }

    @Override
    public void delete(String remotePath) {
        if (StringUtils.isBlank(remotePath)) {
            return;
        }
        FTPClient ftpClient = new FTPClient();
        try {
            connect(ftpClient);
            boolean deleted = ftpClient.deleteFile(remotePath);
            if (!deleted) {
                log.warn("删除FTP附件失败: path={}, reply={}", remotePath, ftpClient.getReplyString());
            }
        } catch (Exception e) {
            log.warn("删除FTP附件异常: path={}, msg={}", remotePath, e.getMessage());
        } finally {
            disconnect(ftpClient);
        }
    }

    private void connect(FTPClient ftpClient) throws IOException {
        ftpClient.setControlEncoding(ftpProperties.getControlEncoding());
        ftpClient.connect(ftpProperties.getHost(), ftpProperties.getPort());
        if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
            throw new BusinessException("FTP连接失败，响应码: " + ftpClient.getReplyCode());
        }
        boolean loginSuccess = ftpClient.login(ftpProperties.getUsername(), ftpProperties.getPassword());
        if (!loginSuccess) {
            throw new BusinessException("FTP登录失败");
        }
        if (Boolean.TRUE.equals(ftpProperties.getPassiveMode())) {
            ftpClient.enterLocalPassiveMode();
        } else {
            ftpClient.enterLocalActiveMode();
        }
    }

    private void ensureDirectory(FTPClient ftpClient, String path) throws IOException {
        String[] directories = path.split("/");
        StringBuilder currentPath = new StringBuilder();
        for (String directory : directories) {
            if (StringUtils.isBlank(directory)) {
                continue;
            }
            currentPath.append("/").append(directory);
            if (!ftpClient.changeWorkingDirectory(currentPath.toString())) {
                boolean created = ftpClient.makeDirectory(currentPath.toString());
                if (!created) {
                    throw new BusinessException("创建FTP目录失败: " + currentPath);
                }
            }
        }
    }

    private String buildFileName(String originName) {
        String suffix = "";
        if (StringUtils.isNotBlank(originName) && originName.contains(".")) {
            suffix = originName.substring(originName.lastIndexOf("."));
        }
        return UUID.randomUUID().toString().replace("-", "") + suffix;
    }

    private String normalizePath(String path) {
        if (StringUtils.isBlank(path)) {
            return "";
        }
        String normalized = path.startsWith("/") ? path : "/" + path;
        return normalized.endsWith("/") ? normalized.substring(0, normalized.length() - 1) : normalized;
    }

    private void disconnect(FTPClient ftpClient) {
        try {
            if (ftpClient.isConnected()) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        } catch (Exception e) {
            log.warn("关闭FTP连接异常: {}", e.getMessage());
        }
    }
}
