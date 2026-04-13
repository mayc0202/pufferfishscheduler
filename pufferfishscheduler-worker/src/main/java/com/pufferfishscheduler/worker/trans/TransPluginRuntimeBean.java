package com.pufferfishscheduler.worker.trans;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.trans.runtime.TransPluginRuntime;
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

    @Override
    public DbDatabase getDatabaseById(Integer id) {
        return dbDatabaseService.getDatabaseById(id);
    }

    @Override
    public String getTransFlowStage(Integer flowId) {
        if (flowId == null) {
            return null;
        }
        TransFlow flow = transFlowMapper.selectById(flowId);
        return flow != null ? flow.getStage() : null;
    }

    @Override
    public void downloadFtpFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCache) {
        throw new BusinessException("Worker 节点未集成 FTP 资源下载，请在 Master 设计流程或使用共享存储。");
    }

    @Override
    public void deleteLocalDirectory(String localPath) {
        // Worker 无 ResourceService 时仅跳过（与原先注释掉的 FTP 上传后处理行为一致）
    }

    @Override
    public void uploadFtpWithNames(Integer dbId, String remotePath, List<String> fileNames) {
        // 无 FTP 上传能力时跳过
    }

    @Override
    public String spellIncrementSql(String databaseType, String sql, String incrementType, String incrementField) {
        AbstractQueryEditor editor = DatabaseEditorFactory.getDatabaseEditor(databaseType);
        return editor.spellIncrementSql(sql, incrementType, incrementField);
    }
}
