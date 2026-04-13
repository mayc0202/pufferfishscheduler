package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.FileVo;
import com.pufferfishscheduler.domain.vo.collect.SheetVo;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransParam;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.util.IOUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.excelinput.ExcelInputField;
import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;

import java.util.List;

/**
 * Excel输入插件构造函数
 */
@Slf4j
public class ExcelInputConstructor extends AbstractStepMetaConstructor {

    /**
     * 设计器 getSheets/getFields 等同一会话内连续请求：首请求已把 FTP 文件落到 inputPath/flowId/，
     * 后续请求仅信任本地缓存即可，避免再次连接 FTP（与 {@link #enableDesignerFtpLocalCacheTrust()} 配对使用）。
     */
    private static final ThreadLocal<Boolean> DESIGNER_FTP_TRUST_LOCAL_CACHE = new ThreadLocal<>();

    public static void enableDesignerFtpLocalCacheTrust() {
        DESIGNER_FTP_TRUST_LOCAL_CACHE.set(Boolean.TRUE);
    }

    public static void disableDesignerFtpLocalCacheTrust() {
        DESIGNER_FTP_TRUST_LOCAL_CACHE.remove();
    }

    private static boolean isDesignerFtpTrustLocalCache() {
        return Boolean.TRUE.equals(DESIGNER_FTP_TRUST_LOCAL_CACHE.get());
    }

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        JSONObject root = JSONObject.parseObject(config);
        if (root == null) {
            throw new BusinessException("组件数据不能为空！");
        }
        JSONObject nested = root.getJSONObject("data");
        JSONObject data;
        if (nested != null && nested.getJSONArray("fileList") != null) {
            data = nested;
        } else {
            data = root;
        }
        String name = StringUtils.isNotBlank(root.getString("name"))
                ? root.getString("name")
                : data.getString("name");
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }

        JSONArray fileList = data.getJSONArray("fileList");
        if (null == fileList || fileList.isEmpty()) {
            throw new BusinessException("步骤" + "【" + name + "】" + "中的文件列表不能为空！");
        }

        String fileSourceType = data.getString("fileSourceType");

        // 工作表列表
        JSONArray sheetList = data.getJSONArray("sheetList");
        if (null == sheetList || sheetList.isEmpty()) {
            sheetList = new JSONArray();
        }

        // 字段列表
        JSONArray fieldList = data.getJSONArray("fieldList");
        if (null == fieldList || fieldList.isEmpty()) {
            fieldList = new JSONArray();
        }

        // 引擎类型（空则 JXL；SAX_POI 在 POI 5.x 下不可用，见 resolveSpreadSheetTypeCompatibleWithPoi5）
        String engine = data.getString("engine");

        String password = data.getString("password");
        Integer readLine = data.getInteger("readLine");
        String assCode = data.getString("assCode");
        Boolean ignoreEmptyRows = data.getBoolean("ignoreEmptyRows");

        // 创建Excel输入插件元数据
        ExcelInputMeta excelInputMeta = new ExcelInputMeta();
        excelInputMeta.setDefault();

        if (null != ignoreEmptyRows) {
            excelInputMeta.setIgnoreEmptyRows(ignoreEmptyRows);
        }

        excelInputMeta.setSpreadSheetType(resolveSpreadSheetTypeCompatibleWithPoi5(engine));
        excelInputMeta.setRowLimit(null == readLine ? 0 : readLine);
        excelInputMeta.setEncoding(assCode);
        excelInputMeta.setPassword(password);

        String[] fileNames = new String[fileList.size()];
        String[] fileMasks = new String[fileList.size()];
        String[] fileExcludeMasks = new String[fileList.size()];

        List<FileVo> fileVoList = fileList.toJavaList(FileVo.class);

        for (int i = 0; i < fileVoList.size(); i++) {
            FileVo vo = fileVoList.get(i);
            if (Constants.FILE_SOURCE_TYPE.FTP_FILE.equals(fileSourceType)) {
                // 本地仅按流程 ID 建目录：inputPath/flowId/文件名
                String localBaseName = ftpLocalBaseFileName(vo.getName());
                fileNames[i] = buildFtpExcelLocalInputFullPath(context.getFlowId(), localBaseName);
            } else {
//                fileNames[i] = filePathConfig.getLocalPath() + File.separator + vo.getName();
            }
            fileMasks[i] = vo.getFileMask();
            fileExcludeMasks[i] = vo.getExcludeMask();
        }

        excelInputMeta.setFileName(fileNames);
        excelInputMeta.setFileMask(fileMasks);
        excelInputMeta.setExcludeFileMask(fileExcludeMasks);

        String[] sheetNames = new String[sheetList.size()];
        int[] startRows = new int[sheetList.size()];
        int[] startColumns = new int[sheetList.size()];

        List<SheetVo> sheetVoList = sheetList.toJavaList(SheetVo.class);
        for (int i = 0; i < sheetVoList.size(); i++) {
            SheetVo vo = sheetVoList.get(i);
            sheetNames[i] = vo.getSheetName();
            startRows[i] = Math.max(vo.getStartRow() - 1, 0);
            startColumns[i] = Math.max(vo.getStartColumn() - 1, 0);
        }

        excelInputMeta.setSheetName(sheetNames);
        excelInputMeta.setStartRow(startRows);
        excelInputMeta.setStartColumn(startColumns);


        List<ExcelInputField> fieldVoList = fieldList.toJavaList(ExcelInputField.class);
        ExcelInputField[] fields = new ExcelInputField[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
            JSONObject jo = fieldList.getJSONObject(i);
            ExcelInputField vo = fieldVoList.get(i);
            vo.setRepeated(null != jo.getBoolean("repeat") ? jo.getBoolean("repeat") : false);
            fields[i] = vo;
        }

        excelInputMeta.setField(fields);

        excelInputMeta.setAddResultFile(true);
        excelInputMeta.setStartsWithHeader(true);
        excelInputMeta.setStopOnEmpty(false);
        excelInputMeta.setAcceptingFilenames(false);

        excelInputMeta.setStrictTypes(false);
        excelInputMeta.setErrorIgnored(false);
        excelInputMeta.setErrorLineSkipped(false);
        excelInputMeta.setBadLineFilesExtension("waring");
        excelInputMeta.setErrorFilesExtension("error");
        excelInputMeta.setLineNumberFilesExtension("line");

        // 从插件注册表中获取Excel输入插件的插件ID
        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, excelInputMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, excelInputMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(excelInputMeta);
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

    /**
     * 执行前处理
     */
    @Override
    public void beforeStep(Integer flowId, String stepId, String stepConfig, List<TransParam> params) {
        JSONObject data = parseStepDataObject(stepConfig);
        if (data == null) {
            return;
        }
        if (!Constants.FILE_SOURCE_TYPE.FTP_FILE.equals(data.getString("fileSourceType"))) {
            return;
        }
        beforeGenerateFtpExcelInputDir(flowId);

        // 在 beforeStep 方法开头调用
        IOUtils.setByteArrayMaxOverride(2_147_483_647); // 设置为 Integer.MAX_VALUE

        JSONArray fileList = data.getJSONArray("fileList");
        if (fileList == null || fileList.isEmpty()) {
            throw new BusinessException("FTP 模式下文件列表不能为空");
        }
        String defaultRemoteDir = data.getString("inputPath");
        Integer dbId = resolveFtpDataSourceId(data);
        if (dbId == null) {
            throw new BusinessException("FTP 模式下数据源不能为空（请配置 dataSourceId 或 dataSource）");
        }
//        ResourceService resourceService = PufferfishSchedulerApplicationContext.getBean(ResourceService.class);
//        List<FileVo> fileVoList = fileList.toJavaList(FileVo.class);
//        for (FileVo vo : fileVoList) {
//            if (StringUtils.isBlank(vo.getName())) {
//                continue;
//            }
//            String remoteDir = resolveFtpRemoteDir(vo.getName(), vo.getPath(), defaultRemoteDir);
//            String remoteFileName = ftpRemoteFileName(vo.getName());
//            if (StringUtils.isBlank(remoteFileName)) {
//                throw new BusinessException("FTP 文件名无效: " + vo.getName());
//            }
//            String localBaseName = ftpLocalBaseFileName(vo.getName());
//            String localFull = buildFtpExcelLocalInputFullPath(flowId, localBaseName);
//            // ResourceService 内会将 file.path.ftp（FTPConfig）与 remoteDir 拼接为完整远程目录
//            resourceService.downloadFileToLocal(
//                    dbId, remoteDir, remoteFileName, localFull, isDesignerFtpTrustLocalCache());
//        }
    }

    /**
     * 执行后处理
     */
    @Override
    public void afterStep(Integer flowId, String stepId, String stepConfig) {
        //恢复 POI 的默认上限（1GB）
        IOUtils.setByteArrayMaxOverride(1_000_000_000);
    }

    /**
     * 执行后处理
     */
    /**
     * Kettle StaxPoi（SAX_POI）依赖旧版 POI 的 XSSFReader API；本工程统一使用 Apache POI 5.x 时会产生 NoSuchMethodError，
     * 故将 SAX_POI 映射为 POI（标准 DOM 读 xlsx，大文件内存占用更高）。
     */
    private static SpreadSheetType resolveSpreadSheetTypeCompatibleWithPoi5(String engine) {
        if (StringUtils.isBlank(engine)) {
            return SpreadSheetType.JXL;
        }
        SpreadSheetType t = SpreadSheetType.valueOf(engine);
        if (t == SpreadSheetType.SAX_POI) {
            log.debug("engine=SAX_POI 与 POI 5.x 不兼容，已改用 POI 读取 xlsx");
            return SpreadSheetType.POI;
        }
        return t;
    }

    private static Integer resolveFtpDataSourceId(JSONObject data) {
        Integer id = data.getInteger("dataSourceId");
        if (id != null) {
            return id;
        }
        String s = data.getString("dataSourceId");
        if (StringUtils.isNotBlank(s)) {
            return Integer.valueOf(s.trim());
        }
        s = data.getString("dataSource");
        if (StringUtils.isNotBlank(s)) {
            return Integer.valueOf(s.trim());
        }
        return null;
    }

    /**
     * fileList.name 在 FTP 下可为「目录/文件名」，与资源浏览一致；本地临时文件只使用最后一段文件名。
     */
    private static String ftpLocalBaseFileName(String nameOrPath) {
        return ftpRemoteFileName(nameOrPath);
    }

    private static String ftpRemoteFileName(String nameOrPath) {
        if (StringUtils.isBlank(nameOrPath)) {
            return "";
        }
        String p = nameOrPath.trim().replace('\\', '/');
        int idx = p.lastIndexOf('/');
        if (idx < 0) {
            return p;
        }
        String file = p.substring(idx + 1);
        return StringUtils.isNotBlank(file) ? file : p;
    }

    /**
     * 远程目录（相对 FTP 根，下载时与 {@code file.path.ftp} 拼接）。优先 FileVo.path，否则从 name 解析父目录，再用 data.inputPath。
     */
    private static String resolveFtpRemoteDir(String nameOrPath, String fileVoPath, String defaultInputPath) {
        if (StringUtils.isNotBlank(fileVoPath)) {
            return fileVoPath;
        }
        if (StringUtils.isBlank(nameOrPath)) {
            return defaultInputPath;
        }
        String p = nameOrPath.trim().replace('\\', '/');
        int idx = p.lastIndexOf('/');
        if (idx <= 0) {
            return defaultInputPath;
        }
        return p.substring(0, idx);
    }
}
