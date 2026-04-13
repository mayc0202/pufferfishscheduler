package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.worker.config.FilePathConfig;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import com.pufferfishscheduler.plugin.FtpUploadStepMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * FTP上传步骤构造器
 */
public class FTPUploadConstructor extends AbstractStepMetaConstructor {

    /**
     * 创建FTP上传步骤元数据
     */
    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取数据库服务、文件路径配置、AES工具
        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        FilePathConfig filePathConfig = PufferfishSchedulerApplicationContext.getBean(FilePathConfig.class);
        AESUtil aesUtil = PufferfishSchedulerApplicationContext.getBean(AESUtil.class);

        JSONObject jsonObject = JSONObject.parseObject(config);
        if (jsonObject == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 从配置中提取组件属性
        String name = jsonObject.getString("name"); // 组件名称
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }

        // 从配置中提取组件数据
        JSONObject data = jsonObject.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 创建FTP上传步骤元数据
        FtpUploadStepMeta ftpUploadStepMeta = new FtpUploadStepMeta();
        ftpUploadStepMeta.setDefault();

        String dataSourceId = data.getString("dataSourceId"); // 数据源ID
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("数据源ID不能为空！");
        }
        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        ftpUploadStepMeta.setHost(database.getDbHost());
        ftpUploadStepMeta.setPort(Long.parseLong(database.getDbPort()));
        ftpUploadStepMeta.setUsername(database.getUsername());
        ftpUploadStepMeta.setPassword(aesUtil.decrypt(database.getPassword()));
        JSONObject properties = JSONObject.parseObject(database.getProperties());
        if (null == properties) {
            ftpUploadStepMeta.setMode("");
            ftpUploadStepMeta.setControlEncoding("");
        } else {
            // 工作模式
            String mode = properties.getString("mode");
            ftpUploadStepMeta.setMode(mode);
            // 控制编码
            String controlEncoding = properties.getString("controlEncoding");
            ftpUploadStepMeta.setControlEncoding(controlEncoding);
        }
        // 服务器类型
        ftpUploadStepMeta.setFtpType(database.getType());

        // 二进制
        Boolean binaryMode = data.getBoolean("binaryMode");
        ftpUploadStepMeta.setBinaryMode(String.valueOf(binaryMode));

        // 通配符
        String wildCard = data.getString("wildCard");
        ftpUploadStepMeta.setWildcard(wildCard);

        // 本地目录
        String localDirectory = data.getString("localDirectory");
        ftpUploadStepMeta.setLocalDirectory(filePathConfig.getLocalPath() + localDirectory);

        // 目标目录
        String remoteDirectory = data.getString("remoteDirectory");
        ftpUploadStepMeta.setRemoteDirectory(remoteDirectory);

        // 移除文件
        Boolean removeLocalFile = data.getBoolean("removeLocalFile");
        ftpUploadStepMeta.setRemoveLocalFile(String.valueOf(removeLocalFile));

        // 覆盖文件
        Boolean overwriteFile = data.getBoolean("overwriteFile");
        ftpUploadStepMeta.setOverwriteFile(String.valueOf(overwriteFile));

        // 超时
        Integer timeOut = data.getInteger("timeOut");
        if (null != timeOut) {
            ftpUploadStepMeta.setTimeout(Long.valueOf(timeOut));
        }

        // 输出字段
        JSONArray fieldLists = data.getJSONArray("outFiledList");
        if (null == fieldLists) {
            fieldLists = new JSONArray();
        }
        int size = fieldLists.size();
        if (size > 0) {
            String[] fieldTable = new String[size];
            for (int i = 0; i < size; i++) {
                JSONObject object = JSONObject.parseObject(fieldLists.get(i).toString());
                JSONObject json = new JSONObject();
                json.put("project", object.getString("name"));
                json.put("field", object.getString("fieldName"));
                fieldTable[i] = json.toJSONString();
            }
            ftpUploadStepMeta.setOutputFieldList(fieldTable);
        }

        // 内置目录
        ftpUploadStepMeta.setRootPath(filePathConfig.getLocalPath());

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, ftpUploadStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, ftpUploadStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(ftpUploadStepMeta);
        }
        Integer copiesCache = data.getInteger("copiesCache");
        if (null != copiesCache) {
            stepMeta.setCopies(copiesCache);
        }
        //判断是否为复制流程
        boolean distributeType = data.getBooleanValue("distributeType");
        if (distributeType) {
            stepMeta.setDistributes(false);
        }

        return stepMeta;
    }
}
