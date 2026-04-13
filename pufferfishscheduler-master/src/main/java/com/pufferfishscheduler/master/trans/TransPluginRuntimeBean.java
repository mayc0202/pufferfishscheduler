package com.pufferfishscheduler.master.trans;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.vo.collect.TransFlowVo;
import com.pufferfishscheduler.master.collect.trans.service.TransFlowService;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.editor.AbstractQueryEditor;
import com.pufferfishscheduler.master.database.editor.DatabaseEditorFactory;
import com.pufferfishscheduler.master.database.resource.service.ResourceService;
import com.pufferfishscheduler.trans.runtime.TransPluginRuntime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Master 侧 {@link TransPluginRuntime} 实现，供 trans-core 步骤构造器通过 ApplicationContext 获取。
 */
@Service
public class TransPluginRuntimeBean implements TransPluginRuntime {

    @Autowired
    private DbDatabaseService dbDatabaseService;
    @Autowired
    private TransFlowService transFlowService;
    @Autowired
    private ResourceService resourceService;

    @Override
    public DbDatabase getDatabaseById(Integer id) {
        return dbDatabaseService.getDatabaseById(id);
    }

    @Override
    public String getTransFlowStage(Integer flowId) {
        if (flowId == null) {
            return null;
        }
        try {
            TransFlowVo vo = transFlowService.detail(flowId);
            return vo != null ? vo.getStage() : null;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void downloadFtpFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCache) {
        resourceService.downloadFileToLocal(dbId, remoteRelativeDir, fileName, localFullPath, trustLocalCache);
    }

    @Override
    public void deleteLocalDirectory(String localPath) {
        resourceService.deleteLocalDirectory(localPath);
    }

    @Override
    public void uploadFtpWithNames(Integer dbId, String remotePath, List<String> fileNames) {
        resourceService.uploadWithNames(dbId, remotePath, fileNames);
    }

    @Override
    public String spellIncrementSql(String databaseType, String sql, String incrementType, String incrementField) {
        AbstractQueryEditor editor = DatabaseEditorFactory.getDatabaseEditor(databaseType);
        return editor.spellIncrementSql(sql, incrementType, incrementField);
    }
}
