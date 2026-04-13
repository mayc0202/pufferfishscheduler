package com.pufferfishscheduler.trans.runtime;

import com.pufferfishscheduler.dao.entity.DbDatabase;

import java.util.List;

/**
 * Master / Worker 侧通过 Spring 注册唯一实现，供转换步骤构造器从 {@link com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext} 获取，
 * 避免 trans-core 直接依赖各模块的 DbDatabaseService、ResourceService、TransFlowService 等类型。
 */
public interface TransPluginRuntime {

    DbDatabase getDatabaseById(Integer id);

    /**
     * @return 转换流 stage；不存在时返回 null
     */
    String getTransFlowStage(Integer flowId);

    void downloadFtpFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCache);

    void deleteLocalDirectory(String localPath);

    void uploadFtpWithNames(Integer dbId, String remotePath, List<String> fileNames);

    /**
     * 表输入等场景的增量 SQL 拼装（由宿主实现委托到各库的 QueryEditor）。
     */
    String spellIncrementSql(String databaseType, String sql, String incrementType, String incrementField);
}
