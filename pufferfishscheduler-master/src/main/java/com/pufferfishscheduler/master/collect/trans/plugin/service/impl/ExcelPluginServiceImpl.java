package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.PoiJvmBootstrap;
import com.pufferfishscheduler.domain.form.collect.TransConfigForm;
import com.pufferfishscheduler.domain.vo.collect.BaseFieldVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.collect.trans.plugin.StepMetaConstructorFactory;
import com.pufferfishscheduler.master.collect.trans.plugin.constructor.ExcelInputConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.service.ExcelPluginService;
import com.pufferfishscheduler.master.collect.trans.service.StepService;
import com.pufferfishscheduler.master.database.resource.service.ResourceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.spreadsheet.KCell;
import org.pentaho.di.core.spreadsheet.KCellType;
import org.pentaho.di.core.spreadsheet.KSheet;
import org.pentaho.di.core.spreadsheet.KWorkbook;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Excel服务实现类
 */
@Slf4j
@Service
public class ExcelPluginServiceImpl implements ExcelPluginService {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private StepService stepService;

    /**
     * 获取所有的文件
     *
     * @param dbId 数据库ID
     * @param path 路径
     * @return {@link List}<{@link ResourceVo}>
     */
    @Override
    public List<ResourceVo> getResources(Integer dbId, String path) {
        if (null == dbId) {
            throw new BusinessException("数据源ID不能为空!");
        }

        return resourceService.getResourceList(dbId, path, List.of(Constants.FILE_TYPE.XLSX, Constants.FILE_TYPE.XLS));
    }

    /**
     * 获取所有的sheet名称
     *
     * @param form 配置表单
     * @return {@link Set}<{@link String}>
     */
    @Override
    public Set<String> getSheets(TransConfigForm form) {
        String config = form.getConfig();
        if (StringUtils.isBlank(config)) {
            return new HashSet<>();
        }

        // 构建转换元数据
        TransMeta transMeta = new TransMeta();
        transMeta.setName("转换");

        // 构建Excel输入元数据（FTP 时 beforeStep 会下载到本地 input/flowId/）
        ExcelInputMeta excelInputMeta = buildExcelInputMeta(transMeta, form);
        FileInputList fileList = excelInputMeta.getFileList(transMeta); // 注意：TransMeta 可以新建
        Set<String> sheets = Sets.newHashSet();

        for (FileObject fileObject : fileList.getFiles()) {
            KWorkbook workbook = null;
            try {
                String pathForPoi = toPlainLocalPathForWorkbook(KettleVFS.getFilename(fileObject));
                if (excelInputMeta.getSpreadSheetType() == SpreadSheetType.POI) {
                    // Kettle PoiWorkbook 依赖旧 POI 的 WorkbookFactory.create(Object)，与 POI 5.x 不兼容
                    try (Workbook wb = openNativePoiWorkbook(pathForPoi, excelInputMeta.getPassword())) {
                        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
                            sheets.add(wb.getSheetName(i));
                        }
                    }
                } else {
                    workbook = org.pentaho.di.trans.steps.excelinput.WorkbookFactory.getWorkbook(
                            excelInputMeta.getSpreadSheetType(),
                            pathForPoi,
                            excelInputMeta.getEncoding(),
                            excelInputMeta.getPassword()
                    );
                    sheets.addAll(Arrays.asList(workbook.getSheetNames()));
                }
            } catch (Exception e) {
                log.error("读取Excel文件失败: {}", fileObject.getName(), e);
                handleWorkbookException(e, String.valueOf(fileObject.getName()));
            } finally {
                if (workbook != null) {
                    try {
                        workbook.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        return sheets;
    }

    /**
     * 处理工作簿异常
     *
     * @param e        异常
     * @param fileName 文件名
     */
    private void handleWorkbookException(Exception e, String fileName) {
        String message = e.getMessage();
        if (message != null && message.contains("empty")) {
            throw new BusinessException("无法读取文件" + fileName + "，该文件是空的！");
        } else if (message != null && message.contains("Password incorrect")) {
            throw new BusinessException("无法读取文件" + fileName + "，请检查解密码是否正确！");
        } else {
            String detail = e.getMessage();
            if (StringUtils.isNotBlank(detail) && detail.length() < 400) {
                throw new BusinessException("无法读取文件" + fileName + "。原因：" + detail);
            }
            throw new BusinessException("无法读取文件" + fileName + "，请检查表格处理引擎以及文件等配置！");
        }
    }

    /**
     * Kettle VFS 常返回 {@code file:///...}，部分环境下 POI/SAX 用该 URI 字符串打开会失败，需转为本地绝对路径。
     */
    private static String toPlainLocalPathForWorkbook(String kettleVfsFilename) {
        if (StringUtils.isBlank(kettleVfsFilename)) {
            return kettleVfsFilename;
        }
        String p = kettleVfsFilename.trim();
        if (!p.startsWith("file:")) {
            return p;
        }
        try {
            return Paths.get(URI.create(p)).normalize().toAbsolutePath().toString();
        } catch (Exception e) {
            log.warn("Workbook 路径规范化失败，使用原路径: {}", kettleVfsFilename, e);
            return p;
        }
    }

    /**
     * 使用 Apache POI 5.x 打开工作簿（与 Kettle PoiWorkbook 解耦，避免 NoSuchMethodError）。
     */
    private static Workbook openNativePoiWorkbook(String path, String password) throws Exception {
        PoiJvmBootstrap.ensureLargeOoxmlReadLimits();
        File f = new File(path);
        if (!f.isFile()) {
            throw new java.io.FileNotFoundException("文件不存在: " + path);
        }
        if (StringUtils.isNotBlank(password)) {
            return org.apache.poi.ss.usermodel.WorkbookFactory.create(f, password, true);
        }
        return org.apache.poi.ss.usermodel.WorkbookFactory.create(f);
    }

    /**
     * 与 {@link #processingWorkbook} 逻辑对齐，使用原生 POI 5 模型解析表头与样例行类型。
     */
    private void processingWorkbookWithNativePoi(RowMetaInterface fields, ExcelInputMeta info, Workbook wb)
            throws KettlePluginException {
        DataFormatter formatter = new DataFormatter();
        int nrSheets = wb.getNumberOfSheets();
        boolean readAllSheets = info.readAllSheets();
        String[] configuredSheets = info.getSheetName();

        for (int sheetIdx = 0; sheetIdx < nrSheets; sheetIdx++) {
            String sheetName = wb.getSheetName(sheetIdx);
            if (!readAllSheets && !Arrays.asList(configuredSheets).contains(sheetName)) {
                continue;
            }

            Sheet sheet = wb.getSheetAt(sheetIdx);

            int startRow;
            int startCol;
            if (readAllSheets) {
                startRow = (info.getStartRow().length > 0) ? info.getStartRow()[0] : 0;
                startCol = (info.getStartColumn().length > 0) ? info.getStartColumn()[0] : 0;
            } else {
                int sheetIndex = Const.indexOfString(sheetName, configuredSheets);
                if (sheetIndex < 0) {
                    continue;
                }
                startRow = info.getStartRow()[sheetIndex];
                startCol = info.getStartColumn()[sheetIndex];
            }

            Row headerRowObj = sheet.getRow(startRow);
            if (headerRowObj == null) {
                log.warn("Sheet '{}' 起始行 {} 无数据，跳过", sheetName, startRow);
                continue;
            }

            int lastCell = headerRowObj.getLastCellNum();
            if (lastCell <= startCol) {
                log.warn("Sheet '{}' 表头行无有效列，跳过", sheetName);
                continue;
            }

            for (int col = startCol; col < lastCell; col++) {
                Cell headerCell = headerRowObj.getCell(col);
                String fieldName = headerCell == null ? "" : formatter.formatCellValue(headerCell).trim();
                if (StringUtils.isEmpty(fieldName)) {
                    continue;
                }

                Row sampleRow = sheet.getRow(startRow + 1);
                Cell sampleCell = sampleRow == null ? null : sampleRow.getCell(col);
                int fieldType = inferCellTypeFromNativePoi(sampleCell);

                if (fieldType != ValueMetaInterface.TYPE_NONE) {
                    ValueMetaInterface field = ValueMetaFactory.createValueMeta(fieldName, fieldType);
                    fields.addValueMeta(field);
                }
            }
        }
    }

    private static int inferCellTypeFromNativePoi(Cell cell) {
        if (cell == null) {
            return ValueMetaInterface.TYPE_STRING;
        }
        CellType t = cell.getCellType();
        if (t == CellType.FORMULA) {
            t = cell.getCachedFormulaResultType();
        }
        switch (t) {
            case BOOLEAN:
                return ValueMetaInterface.TYPE_BOOLEAN;
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return ValueMetaInterface.TYPE_DATE;
                }
                return ValueMetaInterface.TYPE_NUMBER;
            case BLANK:
                return ValueMetaInterface.TYPE_STRING;
            default:
                return ValueMetaInterface.TYPE_STRING;
        }
    }

    /**
     * 获取所有的字段
     *
     * @param form 配置表单
     * @return {@link List}<{@link BaseFieldVo}>
     */
    @Override
    public List<BaseFieldVo> getFields(TransConfigForm form) {
        String config = form.getConfig();
        if (StringUtils.isBlank(config)) {
            return new ArrayList<>();
        }

        // 构建转换元数据
        TransMeta transMeta = new TransMeta();
        transMeta.setName("转换");

        // 构建Excel输入元数据
        ExcelInputMeta excelInputMeta = buildExcelInputMeta(transMeta, form);
        FileInputList fileList = excelInputMeta.getFileList(transMeta);

        RowMetaInterface fields = new RowMeta();

        for (FileObject file : fileList.getFiles()) {
            KWorkbook workbook = null;
            try {
                String pathForPoi = toPlainLocalPathForWorkbook(KettleVFS.getFilename(file));
                if (excelInputMeta.getSpreadSheetType() == SpreadSheetType.POI) {
                    try (Workbook wb = openNativePoiWorkbook(pathForPoi, excelInputMeta.getPassword())) {
                        processingWorkbookWithNativePoi(fields, excelInputMeta, wb);
                    }
                } else {
                    workbook = org.pentaho.di.trans.steps.excelinput.WorkbookFactory.getWorkbook(
                            excelInputMeta.getSpreadSheetType(),
                            pathForPoi,
                            excelInputMeta.getEncoding(),
                            excelInputMeta.getPassword()
                    );
                    processingWorkbook(fields, excelInputMeta, workbook);
                }
                // 只要处理了一个文件就退出循环（假设所有文件结构相同）
                break;
            } catch (Exception e) {
                log.error("解析Excel字段失败: {}", file.getName(), e);
                throw new BusinessException("解析Excel字段失败: " + e.getMessage());
            } finally {
                if (workbook != null) {
                    try {
                        workbook.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        return convertToBaseFieldVoList(fields);
    }

    /**
     * 转换为基础字段VO列表
     *
     * @param fields 字段元数据接口
     * @return {@link List}<{@link BaseFieldVo}>
     */
    private List<BaseFieldVo> convertToBaseFieldVoList(RowMetaInterface fields) {
        List<BaseFieldVo> fieldNames = Lists.newArrayList();
        for (int i = 0; i < fields.size(); i++) {
            ValueMetaInterface field = fields.getValueMeta(i);
            BaseFieldVo excelField = new BaseFieldVo();
            excelField.setName(field.getName());
            excelField.setLength(field.getLength() == -1 ? "" : String.valueOf(field.getLength()));
            excelField.setDecimalSymbol(field.getDecimalSymbol());
            excelField.setCurrencySymbol(field.getCurrencySymbol());
            excelField.setFormat(field.getFormatMask());
            excelField.setGroupSymbol(field.getGroupingSymbol());
            excelField.setPrecision(field.getPrecision() == -1 ? "" : String.valueOf(field.getPrecision()));
            excelField.setRepeat(false);
            excelField.setTrimtype(field.getTrimType());
            excelField.setType(field.getType());
            fieldNames.add(excelField);
        }
        return fieldNames;
    }

    /**
     * 处理工作簿
     *
     * @param fields   字段元数据接口
     * @param info     输入元数据
     * @param workbook 工作簿
     * @throws KettlePluginException 异常
     */
    private void processingWorkbook(RowMetaInterface fields, ExcelInputMeta info, KWorkbook workbook)
            throws KettlePluginException {
        int nrSheets = workbook.getNumberOfSheets();
        boolean readAllSheets = info.readAllSheets();
        String[] configuredSheets = info.getSheetName();

        for (int sheetIdx = 0; sheetIdx < nrSheets; sheetIdx++) {
            String sheetName = workbook.getSheetNames()[sheetIdx];
            // 检查是否需要处理此 sheet
            if (!readAllSheets && !Arrays.asList(configuredSheets).contains(sheetName)) {
                continue;
            }

            KSheet sheet = workbook.getSheet(sheetIdx);

            // 获取当前 sheet 的起始行和列
            int startRow, startCol;
            if (readAllSheets) {
                startRow = (info.getStartRow().length > 0) ? info.getStartRow()[0] : 0;
                startCol = (info.getStartColumn().length > 0) ? info.getStartColumn()[0] : 0;
            } else {
                int sheetIndex = Const.indexOfString(sheetName, configuredSheets);
                if (sheetIndex < 0) continue;
                startRow = info.getStartRow()[sheetIndex];
                startCol = info.getStartColumn()[sheetIndex];
            }

            // 获取表头行（可能抛出异常）
            KCell[] headerRow;
            try {
                headerRow = sheet.getRow(startRow);
            } catch (ArrayIndexOutOfBoundsException e) {
                // 起始行超出工作表范围，跳过该 sheet
                log.warn("Sheet '{}' 起始行 {} 超出范围，跳过", sheetName, startRow);
                continue;
            }
            if (headerRow == null || headerRow.length <= startCol) {
                log.warn("Sheet '{}' 表头行无有效列，跳过", sheetName);
                continue;
            }

            // 遍历从 startCol 开始的列
            for (int col = startCol; col < headerRow.length; col++) {
                KCell headerCell = headerRow[col];
                if (headerCell == null || headerCell.getType() == KCellType.EMPTY) {
                    // 允许空列，但不跳过后续列（因为可能有非空列）
                    continue;
                }
                String fieldName = headerCell.getContents();
                if (StringUtils.isEmpty(fieldName)) {
                    continue;
                }

                // 推断字段类型：查看下一行同一列
                KCell sampleCell = null;
                try {
                    sampleCell = sheet.getCell(col, startRow + 1);
                } catch (ArrayIndexOutOfBoundsException ignored) {
                    // 下一行不存在，默认字符串类型
                }
                int fieldType = inferCellType(sampleCell);

                if (fieldType != ValueMetaInterface.TYPE_NONE) {
                    ValueMetaInterface field = ValueMetaFactory.createValueMeta(fieldName, fieldType);
                    fields.addValueMeta(field);
                }
            }
        }
    }

    /**
     * 推断单元格类型
     *
     * @param cell 单元格
     * @return 单元格类型
     */
    private int inferCellType(KCell cell) {
        if (cell == null) return ValueMetaInterface.TYPE_STRING;
        switch (cell.getType()) {
            case BOOLEAN:
                return ValueMetaInterface.TYPE_BOOLEAN;
            case DATE:
                return ValueMetaInterface.TYPE_DATE;
            case NUMBER:
                return ValueMetaInterface.TYPE_NUMBER;
            default:
                return ValueMetaInterface.TYPE_STRING;
        }
    }

    /**
     * 构建Excel输入元数据
     *
     * @param transMeta 转换元数据
     * @param form 配置表单
     * @return {@link ExcelInputMeta}
     */
    private ExcelInputMeta buildExcelInputMeta(TransMeta transMeta, TransConfigForm form) {
        PluginRegistry registryID = PluginRegistry.getInstance();

        String dataOnly = toStepDataJson(form.getConfig());

        StepContext context = new StepContext();
        context.setRegistryID(registryID);
        context.setStepMetaMap(new HashMap<>());
        context.setFlowId(form.getFlowId());
        context.setId(AbstractStepMetaConstructor.EXCEL_PREVIEW_STEP_ID);

        // 设置参数
        List<TransParam> params = Lists.newArrayList();
        stepService.setParams(form.getFlowId(), transMeta, params);

        AbstractStepMetaConstructor constructor = StepMetaConstructorFactory.getConstructor(Constants.StepMetaType.EXCEL_INPUT);
        // 设计器连续请求：先 getSheets 再 getFields 时不再为比对远程大小而重复连 FTP（本地已有合法缓存则直接复用）
        ExcelInputConstructor.enableDesignerFtpLocalCacheTrust();
        try {
            constructor.beforeStep(form.getFlowId(), null, dataOnly, params);
            StepMeta stepMeta = constructor.create(form.getConfig(), transMeta, context);
            stepMeta.setDraw(false);
            transMeta.addStep(stepMeta);
            return (ExcelInputMeta) stepMeta.getStepMetaInterface();
        } finally {
            ExcelInputConstructor.disableDesignerFtpLocalCacheTrust();
        }
    }

    /**
     * 设计器传入的 config 可能为「带 name/data 的完整组件 JSON」，beforeStep/afterStep 需要 data 段
     */
    private static String toStepDataJson(String config) {
        if (StringUtils.isBlank(config)) {
            return config;
        }
        JSONObject root = JSONObject.parseObject(config);
        if (root != null && root.containsKey("data")) {
            JSONObject d = root.getJSONObject("data");
            return d != null ? d.toJSONString() : config;
        }
        return config;
    }

}
