package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.plugin.FtpDownloadStepMeta;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

/**
 * FTP下载构造器
 */
public class FTPDownloadConstructor extends AbstractStepMetaConstructor {


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
            throw new BusinessException("【" + name + "】组件数据不能为空！");
        }

        // 创建FTP下载步骤元数据
        FtpDownloadStepMeta ftpDownloadStepMeta = new FtpDownloadStepMeta();
        ftpDownloadStepMeta.setDefault();

        String dataSourceId = data.getString("dataSourceId"); // 数据源ID
        if (StringUtils.isBlank(dataSourceId)) {
            throw new BusinessException("【" + name + "】数据源ID不能为空！");
        }
        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("【" + name + "】数据源不存在!");
        }

        ftpDownloadStepMeta.setHost(database.getDbHost());
        ftpDownloadStepMeta.setPort(Long.parseLong(database.getDbPort()));
        ftpDownloadStepMeta.setUsername(database.getUsername());
        ftpDownloadStepMeta.setPassword(aesUtil.decrypt(database.getPassword()));
        JSONObject properties = JSONObject.parseObject(database.getProperties());
        if (null == properties) {
            ftpDownloadStepMeta.setMode("");
            ftpDownloadStepMeta.setControlEncoding("");
        } else {
            // 工作模式
            String mode = properties.getString("mode");
            ftpDownloadStepMeta.setMode(mode);
            // 控制编码
            String controlEncoding = properties.getString("controlEncoding");
            ftpDownloadStepMeta.setControlEncoding(controlEncoding);
        }
        // 服务器类型
        ftpDownloadStepMeta.setFtpType(database.getType());


        // 二进制
        Boolean binaryMode = data.getBoolean("binaryMode");
        ftpDownloadStepMeta.setBinaryMode(String.valueOf(binaryMode));

        // 通配符
        String wildCard = data.getString("wildCard");
        ftpDownloadStepMeta.setWildcard(wildCard);

        // 本地目录

        String localDirectory = data.getString("localDirectory");
        ftpDownloadStepMeta.setLocalDirectory(filePathConfig.getLocalPath() + localDirectory);

        // 目标目录
        String remoteDirectory = data.getString("ftpDirectory");
        ftpDownloadStepMeta.setRemoteDirectory(remoteDirectory);

        // 文件名包含日期
        Boolean dateInFilename = data.getBoolean("dateInFilename");
        ftpDownloadStepMeta.setDateInFilename(String.valueOf(dateInFilename));

        // 日期格式
        String dateTimeFormat = data.getString("dateTimeFormat");
        ftpDownloadStepMeta.setDateTimeFormat(dateTimeFormat);

        // 移动文件
        Boolean moveFile = data.getBoolean("moveFile");
        ftpDownloadStepMeta.setMoveFile(String.valueOf(moveFile));

        // 不存在就新建
        Boolean createNewFolder = data.getBoolean("createNewFolder");
        ftpDownloadStepMeta.setCreateNewFolder(String.valueOf(createNewFolder));

        // 移动的文件夹
        String moveToDirectory = data.getString("moveToDirectory");
        ftpDownloadStepMeta.setMoveToDirectory(moveToDirectory);

        // 超时
        Integer timeOut = data.getInteger("timeOut");
        if (null != timeOut) {
            ftpDownloadStepMeta.setTimeout(Long.valueOf(timeOut));
        }

        // 移除文件
        Boolean removeFile = data.getBoolean("removeFile");
        ftpDownloadStepMeta.setRemoveFile(String.valueOf(removeFile));

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
            ftpDownloadStepMeta.setOutputFieldList(fieldTable);
        }

        // 内置目录
        ftpDownloadStepMeta.setRootPath(filePathConfig.getLocalPath());

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, ftpDownloadStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, ftpDownloadStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(ftpDownloadStepMeta);
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
