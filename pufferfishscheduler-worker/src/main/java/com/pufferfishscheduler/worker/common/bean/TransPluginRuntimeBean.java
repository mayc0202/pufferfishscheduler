package com.pufferfishscheduler.worker.common.bean;

import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.trans.runtime.TransPluginRuntime;
import com.pufferfishscheduler.worker.common.service.ResourceService;
import com.pufferfishscheduler.worker.task.metadata.editor.AbstractQueryEditor;
import com.pufferfishscheduler.worker.task.metadata.editor.DatabaseEditorFactory;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Worker 侧 {@link TransPluginRuntime} 实现。
 * <p>FTP 资源类能力与 Master 的 ResourceService 不对齐处抛出明确异常，避免静默失败。</p>
 */
@Service
public class TransPluginRuntimeBean implements TransPluginRuntime {

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private ResourceService resourceService;

    @Override
    public DbDatabase getDatabaseById(Integer id) {
        return dbDatabaseService.getDatabaseById(id);
    }

    /**
     * 获取转换流程当前状态。
     */
    @Override
    public String getTransFlowStage(Integer flowId) {
        if (flowId == null) {
            return null;
        }
        TransFlow flow = transFlowMapper.selectById(flowId);
        return flow != null ? flow.getStage() : null;
    }

    /**
     * 从 FTP 下载文件到本地。
     */
    @Override
    public void downloadFtpFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCache) {
        resourceService.downloadFileToLocal(dbId, remoteRelativeDir, fileName, localFullPath, trustLocalCache);
    }

    /**
     * 删除本地目录。
     */
    @Override
    public void deleteLocalDirectory(String localPath) {
        resourceService.deleteLocalDirectory(localPath);
    }

    /**
     * 上传文件并指定文件名
     */
    @Override
    public void uploadFtpWithNames(Integer dbId, String remotePath, List<String> fileNames) {
        resourceService.uploadWithNames(dbId, remotePath, fileNames);
    }

    /**
     * 表输入等场景的增量 SQL 拼装（由宿主实现委托到各库的 QueryEditor）。
     */
    @Override
    public String spellIncrementSql(String databaseType, String sql, String incrementType, String incrementField) {
        AbstractQueryEditor editor = DatabaseEditorFactory.getDatabaseEditor(databaseType);
        return editor.spellIncrementSql(sql, incrementType, incrementField);
    }
}
