package com.pufferfishscheduler.common.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * Excel通用工具类
 * 提供模板下载、Excel导入解析通用能力
 *
 * @author lsd
 */
@Slf4j
public class ExcelUtil {

    /**
     * 模板统一存放根目录（resource下）
     */
    private static final String TEMPLATE_ROOT_DIR = "template/";

    private ExcelUtil() {
        // 工具类禁用实例化
        throw new AssertionError("工具类不可实例化");
    }

    // ============================== 模板导出 ==============================

    /**
     * 下载resource/template目录下的Excel模板
     *
     * @param response       响应对象
     * @param templateFileName 模板文件名（eg: 值映射模板.xlsx）
     */
    public static void downloadTemplate(HttpServletResponse response, String templateFileName) {
        // 拼接完整模板路径：template/xxx.xlsx
        String templatePath = TEMPLATE_ROOT_DIR + templateFileName;

        try {
            // 设置响应头
            setExcelResponseHeader(response, templateFileName);

            // 获取模板文件流
            InputStream templateStream = ExcelUtil.class.getClassLoader().getResourceAsStream(templatePath);
            if (templateStream == null) {
                throw new BusinessException("模板文件不存在：" + templatePath);
            }

            // 写入模板到响应流
            EasyExcel.write(response.getOutputStream())
                    .excelType(ExcelTypeEnum.XLSX)
                    .withTemplate(templateStream)
                    .build()
                    .finish();

        } catch (Exception e) {
            log.error("模板下载失败：{}", e.getMessage(), e);
            // 下载接口：尽量避免继续走 JSON 异常处理器（此时 Content-Type 已是 xlsx，会二次触发 converter 异常）
            try {
                if (!response.isCommitted()) {
                    response.resetBuffer();
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    response.setCharacterEncoding(Constants.CONTROL_ENCODING);
                    response.setContentType(Constants.CONTENT_TYPE.TXT);
                    PrintWriter writer = response.getWriter();
                    writer.write("模板下载失败：" + e.getMessage());
                    writer.flush();
                    return;
                }
                // 响应已提交：无法再安全切换 Content-Type，也不应该让全局异常器再尝试写 JSON
                return;
            } catch (Exception ignore) {
                // ignore
            }
            throw new BusinessException("模板下载失败：" + e.getMessage());
        }
    }

    // ============================== Excel导入解析 ==============================

    /**
     * 读取上传的Excel文件，返回行数据列表（通用Map结构）
     * 第一行作为表头，后续每行转为Map<表头名, 单元格值>
     *
     * @param file 上传的Excel文件
     * @return 解析后的数据列表
     */
    public static List<Map<String, Object>> readExcel(MultipartFile file) {
        try {
            return EasyExcel.read(file.getInputStream())
                    .sheet()
                    .doReadSync()
                    .stream()
                    .map(row -> {
                        if (row instanceof Map<?, ?> rowMap) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> result = (Map<String, Object>) rowMap;
                            return result;
                        }
                        throw new BusinessException("Excel解析失败：行数据结构不合法");
                    })
                    .toList();
        } catch (IOException e) {
            log.error("Excel文件读取失败：{}", e.getMessage(), e);
            throw new BusinessException("Excel文件读取失败：" + e.getMessage());
        }
    }

    /**
     * 专用：解析值映射模板Excel
     * 适配业务：code、name、valueName三列
     *
     * @param file 上传的文件
     * @return 业务数据列表
     */
    public static List<Map<String, Object>> importValueMappingTemplate(MultipartFile file) {
        List<Map<String, Object>> dataList = readExcel(file);
        if (dataList.isEmpty()) {
            throw new BusinessException("Excel文件无有效数据");
        }
        return dataList;
    }

    // ============================== 私有工具方法 ==============================

    /**
     * 设置Excel下载响应头
     */
    private static void setExcelResponseHeader(HttpServletResponse response, String fileName) {
        try {
            response.setContentType(Constants.CONTENT_TYPE.XLS);
            response.setCharacterEncoding(Constants.CONTROL_ENCODING);
            String encodeName = URLEncoder.encode(fileName, Constants.CONTROL_ENCODING).replace("+", "%20");
            response.setHeader(Constants.HEADER_CONFIG.CONTENT_DISPOSITION, "attachment;filename=" + encodeName);
        } catch (Exception e) {
            log.error("响应头设置失败：{}", e.getMessage(), e);
            throw new BusinessException("响应头设置失败：" + e.getMessage());
        }
    }
}