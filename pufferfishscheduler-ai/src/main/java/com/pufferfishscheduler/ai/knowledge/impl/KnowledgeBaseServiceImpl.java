package com.pufferfishscheduler.ai.knowledge.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.ai.knowledge.model.KnowledgeCreateCommand;
import com.pufferfishscheduler.ai.knowledge.KnowledgeBaseService;
import com.pufferfishscheduler.ai.knowledge.storage.KnowledgeFileStorage;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 *
 * @author Mayc
 * @since 2026-02-22  12:53
 */
@Slf4j
@Service
public class KnowledgeBaseServiceImpl implements KnowledgeBaseService {

    // 设置每批处理的最大文档数
    private final int BATCH_SIZE = 10;

    private final VectorStore vectorStore;
    private final JdbcTemplate jdbcTemplate;
    private final KnowledgeFileStorage knowledgeFileStorage;
    private final ObjectMapper objectMapper;

    public KnowledgeBaseServiceImpl(VectorStore vectorStore,
                                    JdbcTemplate jdbcTemplate,
                                    KnowledgeFileStorage knowledgeFileStorage,
                                    ObjectMapper objectMapper) {
        this.vectorStore = vectorStore;
        this.jdbcTemplate = jdbcTemplate;
        this.knowledgeFileStorage = knowledgeFileStorage;
        this.objectMapper = objectMapper;
    }

    /**
     * 文件切片
     * @param file
     */
    @Override
    public void fileChunk(MultipartFile file) {
        String fileName = file.getOriginalFilename();
        String fileExtension = getFileExtension(fileName).toLowerCase();

        log.info("开始解析文件: {}, 类型: {}", fileName, fileExtension);

        try {
            // 根据文件类型选择解析方法
            List<Document> documents = switch (fileExtension) {
                case Constants.FILE_TYPE.CSV -> parseCsv(file);
                case Constants.FILE_TYPE.XLSX, Constants.FILE_TYPE.XLS -> parseExcel(file);
                case Constants.FILE_TYPE.PDF -> parsePdf(file);
                case Constants.FILE_TYPE.DOCX -> parseDocx(file);
                case Constants.FILE_TYPE.DOC -> parseDoc(file);
                case Constants.FILE_TYPE.TXT, Constants.FILE_TYPE.MD -> parseTxt(file);
                default -> throw new BusinessException("暂不支持的文件类型: " + fileExtension);
            };

            // 分批存储到向量库
            storeInBatches(documents, fileName);

            log.info("解析文件结束: {}, 类型: {}", fileName, fileExtension);
        } catch (Exception e) {
            log.error("文件切片失败：{}", e.getMessage(), e);
            throw new BusinessException("文件切片失败：" + e.getMessage());
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public String createKnowledge(KnowledgeCreateCommand command, List<MultipartFile> files) {
        validateCreateCommand(command);
        Date now = new Date();
        String documentId = CommonUtil.getUUIDString();
        String operator = StringUtils.hasText(command.getOperator()) ? command.getOperator() : "system";

        jdbcTemplate.update("""
                INSERT INTO kb_document
                (id, title, category_id, status, content, tags_json, has_attachment, chunk_status, chunk_count, vector_namespace, deleted, created_by, created_time, updated_by, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                documentId,
                command.getTitle(),
                command.getCategoryId(),
                command.getStatus() == null ? 1 : command.getStatus(),
                command.getContent(),
                toJson(command.getTags()),
                CollectionUtils.isEmpty(files) ? 0 : 1,
                0,
                0,
                command.getVectorNamespace(),
                0,
                operator,
                now,
                operator,
                now
        );

        // 正文有内容时，直接按文本切片后入向量库并写切片索引
        if (StringUtils.hasText(command.getContent())) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("source", command.getTitle());
            metadata.put("document_id", documentId);
            List<Document> contentChunks = splitLongText(command.getContent(), metadata, 1);
            List<String> contentVectorIds = storeInBatches(contentChunks, command.getTitle());
            persistChunkIndex(documentId, null, contentChunks, contentVectorIds, operator, now);
        }

        if (!CollectionUtils.isEmpty(files)) {
            for (MultipartFile file : files) {
                String attachmentId = saveAttachment(documentId, file, operator, now);
                List<Document> documents = parseByFileType(file);
                List<String> vectorIds = storeInBatches(documents, file.getOriginalFilename());
                persistChunkIndex(documentId, attachmentId, documents, vectorIds, operator, now);
            }
        }

        updateDocumentChunkInfo(documentId, operator, now);
        return documentId;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteKnowledge(String documentId) {
        if (!StringUtils.hasText(documentId)) {
            throw new BusinessException("知识ID不能为空");
        }

        Integer exists = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM kb_document WHERE id = ? AND deleted = 0",
                Integer.class,
                documentId
        );
        if (exists == null || exists == 0) {
            return;
        }

        // 1. 删除FTP附件（失败不抛出，避免中断向量库和DB清理）
        List<String> attachmentPaths = jdbcTemplate.queryForList(
                "SELECT ftp_path FROM kb_document_attachment WHERE document_id = ? AND deleted = 0",
                String.class,
                documentId
        );
        for (String path : attachmentPaths) {
            knowledgeFileStorage.delete(path);
        }
        jdbcTemplate.update(
                "UPDATE kb_document_attachment SET deleted = 1, updated_time = ? WHERE document_id = ? AND deleted = 0",
                new Date(),
                documentId
        );

        // 2. 删除向量库
        List<String> vectorIds = jdbcTemplate.queryForList(
                "SELECT vector_id FROM kb_document_chunk WHERE document_id = ? AND deleted = 0",
                String.class,
                documentId
        ).stream().filter(StringUtils::hasText).toList();
        if (!vectorIds.isEmpty()) {
            vectorStore.delete(vectorIds);
        }

        // 3. 软删切片索引
        jdbcTemplate.update(
                "UPDATE kb_document_chunk SET deleted = 1, updated_time = ? WHERE document_id = ? AND deleted = 0",
                new Date(),
                documentId
        );

        // 4. 软删知识主表
        jdbcTemplate.update(
                "UPDATE kb_document SET deleted = 1, chunk_status = 0, chunk_count = 0, updated_time = ? WHERE id = ? AND deleted = 0",
                new Date(),
                documentId
        );
    }

    private void validateCreateCommand(KnowledgeCreateCommand command) {
        if (command == null) {
            throw new BusinessException("知识参数不能为空");
        }
        if (!StringUtils.hasText(command.getTitle())) {
            throw new BusinessException("知识标题不能为空");
        }
    }

    private String saveAttachment(String documentId, MultipartFile file, String operator, Date now) {
        String attachmentId = CommonUtil.getUUIDString();
        String relativeDir = buildRelativeDir(documentId);
        String ftpPath = knowledgeFileStorage.upload(file, relativeDir);

        jdbcTemplate.update("""
                INSERT INTO kb_document_attachment
                (id, document_id, file_name, file_ext, mime_type, file_size, ftp_path, parse_status, deleted, created_by, created_time, updated_by, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                attachmentId,
                documentId,
                file.getOriginalFilename(),
                getFileExtension(file.getOriginalFilename()),
                file.getContentType(),
                file.getSize(),
                ftpPath,
                0,
                0,
                operator,
                now,
                operator,
                now
        );
        return attachmentId;
    }

    private String buildRelativeDir(String documentId) {
        LocalDate now = LocalDate.now();
        return "/" + now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd")) + "/" + documentId;
    }

    private List<Document> parseByFileType(MultipartFile file) {
        String fileName = file.getOriginalFilename();
        String fileExtension = getFileExtension(fileName).toLowerCase();
        try {
            return switch (fileExtension) {
                case Constants.FILE_TYPE.CSV -> parseCsv(file);
                case Constants.FILE_TYPE.XLSX, Constants.FILE_TYPE.XLS -> parseExcel(file);
                case Constants.FILE_TYPE.PDF -> parsePdf(file);
                case Constants.FILE_TYPE.DOCX -> parseDocx(file);
                case Constants.FILE_TYPE.DOC -> parseDoc(file);
                case Constants.FILE_TYPE.TXT, Constants.FILE_TYPE.MD -> parseTxt(file);
                default -> throw new BusinessException("暂不支持的文件类型: " + fileExtension);
            };
        } catch (IOException e) {
            throw new BusinessException("文件解析失败: " + e.getMessage());
        }
    }

    private void persistChunkIndex(String documentId,
                                   String attachmentId,
                                   List<Document> documents,
                                   List<String> vectorIds,
                                   String operator,
                                   Date now) {
        if (CollectionUtils.isEmpty(vectorIds)) {
            return;
        }
        for (int i = 0; i < vectorIds.size(); i++) {
            Map<String, Object> metadata = i < documents.size() ? documents.get(i).getMetadata() : Collections.emptyMap();
            jdbcTemplate.update("""
                    INSERT INTO kb_document_chunk
                    (id, document_id, attachment_id, chunk_no, vector_id, metadata_json, deleted, created_by, created_time, updated_by, updated_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    CommonUtil.getUUIDString(),
                    documentId,
                    attachmentId,
                    i + 1,
                    vectorIds.get(i),
                    toJson(metadata),
                    0,
                    operator,
                    now,
                    operator,
                    now
            );
        }
    }

    private void updateDocumentChunkInfo(String documentId, String operator, Date now) {
        Integer chunkCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM kb_document_chunk WHERE document_id = ? AND deleted = 0",
                Integer.class,
                documentId
        );
        jdbcTemplate.update(
                "UPDATE kb_document SET chunk_count = ?, chunk_status = 2, updated_by = ?, updated_time = ? WHERE id = ?",
                chunkCount == null ? 0 : chunkCount,
                operator,
                now,
                documentId
        );
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new BusinessException("JSON序列化失败: " + e.getMessage());
        }
    }

    /**
     * 解析CSV文件 - 完整修正版
     */
    private List<Document> parseCsv(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        // 1. 先将文件内容读取到内存（解决流只能读取一次的问题）
        byte[] fileContent = file.getBytes();

        // 2. 检测文件编码
        String encoding = detectEncodingWithTika(fileContent);
        log.info("检测到CSV文件编码: {}", encoding);

        // 3. 使用检测到的编码解析CSV
        try (ByteArrayInputStream bais = new ByteArrayInputStream(fileContent);
             InputStreamReader reader = new InputStreamReader(bais, encoding);
             CSVParser csvParser = CSVFormat.DEFAULT
                     .builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreHeaderCase(true)
                     .setTrim(true)
                     .build()
                     .parse(reader)) {

            // 获取并记录表头信息
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            log.info("CSV文件表头: {}", headerMap.keySet());

            int rowNum = 0;
            for (CSVRecord record : csvParser) {
                rowNum++;

                // 创建元数据
                Map<String, Object> metadata = createBaseMetadata(file, rowNum);
                metadata.put("encoding", encoding);
                metadata.put("row_number", rowNum);
                metadata.put("file_type", "csv");

                // 构建文档内容
                StringBuilder content = new StringBuilder();
                Map<String, String> recordMap = record.toMap();

                // 特别处理"问题"和"回答"列，使内容更清晰
                String question = recordMap.get("问题");
                String answer = recordMap.get("回答");

                if (question != null && answer != null) {
                    // 如果有标准的问题回答列，使用更清晰的格式
                    content.append("问题：").append(question).append("\n");
                    content.append("回答：").append(answer).append("\n");

                    // 在元数据中也记录问题和回答
                    metadata.put("question", question);
                    metadata.put("answer_preview", answer.length() > 50 ? answer.substring(0, 50) + "..." : answer);
                } else {
                    // 如果没有标准列，使用通用格式
                    for (Map.Entry<String, String> entry : recordMap.entrySet()) {
                        content.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                    }
                }

                // 创建文档
                Document doc = new Document(
                        CommonUtil.getUUIDString(),
                        content.toString(),
                        metadata
                );

                documents.add(doc);

                // 可选：每100行打印一次进度
                if (rowNum % 100 == 0) {
                    log.info("已解析 {} 行", rowNum);
                }
            }

            log.info("CSV解析完成，共 {} 行，使用编码: {}", documents.size(), encoding);
        }

        return documents;
    }

    /**
     * 使用Tika检测编码
     */
    private String detectEncodingWithTika(byte[] content) {
        try {
            org.apache.tika.parser.txt.CharsetDetector detector = new org.apache.tika.parser.txt.CharsetDetector();
            detector.setText(content);
            org.apache.tika.parser.txt.CharsetMatch match = detector.detect();
            if (match != null) {
                String encoding = match.getName();
                log.debug("Tika检测到编码: {}, 置信度: {}", encoding, match.getConfidence());
                return encoding;
            }
        } catch (Exception e) {
            log.warn("Tika编码检测失败", e);
        }

        // 如果Tika检测失败，尝试手动检测
        return detectEncodingManually(content);
    }

    /**
     * 手动检测编码（备用方案）- 修正版
     */
    private String detectEncodingManually(byte[] content) {
        // 检查BOM
        if (content.length >= 3 && content[0] == (byte)0xEF && content[1] == (byte)0xBB && content[2] == (byte)0xBF) {
            return "UTF-8";
        } else if (content.length >= 2 && content[0] == (byte)0xFE && content[1] == (byte)0xFF) {
            return "UTF-16BE";
        } else if (content.length >= 2 && content[0] == (byte)0xFF && content[1] == (byte)0xFE) {
            return "UTF-16LE";
        }

        // 尝试常用编码
        String[] encodings = {"GBK", "UTF-8", "GB2312", "GB18030", "ISO-8859-1"};

        for (String encoding : encodings) {
            try {
                // 修正：先创建一个指定编码的字符串，然后截取前1024个字符
                String fullText = new String(content, encoding);
                String sample = fullText.length() > 1024 ? fullText.substring(0, 1024) : fullText;

                // 如果能解码且包含中文字符，则认为是该编码
                if (containsChinese(sample)) {
                    log.debug("手动检测到编码: {}", encoding);
                    return encoding;
                }
            } catch (Exception e) {
                // 忽略解码错误
                log.error("解码错误：{}", e.getMessage());
            }
        }

        // 默认返回UTF-8
        log.debug("使用默认编码: UTF-8");
        return "UTF-8";
    }

    /**
     * 判断字符串是否包含中文
     */
    private boolean containsChinese(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }

        for (char c : str.toCharArray()) {
            Character.UnicodeScript script = Character.UnicodeScript.of(c);
            if (script == Character.UnicodeScript.HAN) {
                return true;
            }
        }
        return false;
    }

    /**
     * 解析Excel文件
     */
    private List<Document> parseExcel(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (Workbook workbook = WorkbookFactory.create(file.getInputStream())) {
            int sheetNum = 0;
            for (Sheet sheet : workbook) {
                sheetNum++;

                // 获取表头（第一行）
                Row headerRow = sheet.getRow(0);
                Map<Integer, String> headers = new HashMap<>();
                if (headerRow != null) {
                    for (Cell cell : headerRow) {
                        headers.put(cell.getColumnIndex(), cell.getStringCellValue());
                    }
                }

                // 遍历数据行
                int rowNum = 1; // 从第2行开始（跳过表头）
                for (Row row : sheet) {
                    if (row.getRowNum() == 0) continue; // 跳过表头

                    StringBuilder content = new StringBuilder();
                    Map<String, Object> metadata = createBaseMetadata(file, rowNum);
                    metadata.put("sheet_name", sheet.getSheetName());
                    metadata.put("sheet_index", sheetNum);

                    for (Cell cell : row) {
                        String header = headers.getOrDefault(cell.getColumnIndex(), "列" + cell.getColumnIndex());
                        String value = getCellValueAsString(cell);
                        content.append(header).append(": ").append(value).append("\n");
                    }

                    Document doc = new Document(UUID.randomUUID().toString(), content.toString(), metadata);
                    documents.add(doc);
                    rowNum++;
                }
            }
        }

        return documents;
    }

    /**
     * 解析PDF文件
     */
    private List<Document> parsePdf(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (PDDocument document = PDDocument.load(file.getInputStream())) {
            PDFTextStripper stripper = new PDFTextStripper();

            // 按页处理
            int totalPages = document.getNumberOfPages();
            for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
                stripper.setStartPage(pageNum);
                stripper.setEndPage(pageNum);

                String pageContent = stripper.getText(document);

                if (!pageContent.trim().isEmpty()) {
                    Map<String, Object> metadata = createBaseMetadata(file, pageNum);
                    metadata.put("page_number", pageNum);
                    metadata.put("total_pages", totalPages);

                    // 如果页面内容太长，可以进一步切分
                    List<Document> pageChunks = splitLongText(pageContent, metadata, pageNum);
                    documents.addAll(pageChunks);
                }
            }
        }

        return documents;
    }

    /**
     * 解析DOCX文件
     */
    private List<Document> parseDocx(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (XWPFDocument document = new XWPFDocument(file.getInputStream())) {
            StringBuilder content = new StringBuilder();
            int paragraphNum = 0;

            for (XWPFParagraph paragraph : document.getParagraphs()) {
                String text = paragraph.getText();
                if (!text.trim().isEmpty()) {
                    content.append(text).append("\n");
                    paragraphNum++;

                    // 每10个段落作为一个文档（可以调整）
                    if (paragraphNum % 10 == 0) {
                        Map<String, Object> metadata = createBaseMetadata(file, paragraphNum / 10);
                        Document doc = new Document(
                                UUID.randomUUID().toString(),
                                content.toString(),
                                metadata
                        );
                        documents.add(doc);
                        content = new StringBuilder();
                    }
                }
            }

            // 处理剩余内容
            if (content.length() > 0) {
                Map<String, Object> metadata = createBaseMetadata(file, (paragraphNum / 10) + 1);
                Document doc = new Document(UUID.randomUUID().toString(), content.toString(), metadata);
                documents.add(doc);
            }
        }

        return documents;
    }

    /**
     * 解析DOC文件（旧格式）
     */
    private List<Document> parseDoc(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (InputStream is = file.getInputStream();
             HWPFDocument document = new HWPFDocument(is);
             WordExtractor extractor = new WordExtractor(document)) {
            String[] paragraphs = extractor.getParagraphText();

            StringBuilder content = new StringBuilder();
            for (int i = 0; i < paragraphs.length; i++) {
                content.append(paragraphs[i]).append("\n");

                if ((i + 1) % 10 == 0 || i == paragraphs.length - 1) {
                    Map<String, Object> metadata = createBaseMetadata(file, (i / 10) + 1);
                    Document doc = new Document(
                            UUID.randomUUID().toString(),
                            content.toString(),
                            metadata
                    );
                    documents.add(doc);
                    content = new StringBuilder();
                }
            }
        }

        return documents;
    }

    /**
     * 解析TXT文件
     */
    private List<Document> parseTxt(MultipartFile file) throws IOException {
        List<Document> documents = new ArrayList<>();

        try (Scanner scanner = new Scanner(file.getInputStream(), "UTF-8")) {
            StringBuilder content = new StringBuilder();
            int lineNum = 0;
            int chunkNum = 0;

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                content.append(line).append("\n");
                lineNum++;

                // 每50行作为一个文档
                if (lineNum % 50 == 0) {
                    chunkNum++;
                    Map<String, Object> metadata = createBaseMetadata(file, chunkNum);
                    Document doc = new Document(
                            UUID.randomUUID().toString(),
                            content.toString(),
                            metadata
                    );
                    documents.add(doc);
                    content = new StringBuilder();
                }
            }

            // 处理剩余内容
            if (content.length() > 0) {
                chunkNum++;
                Map<String, Object> metadata = createBaseMetadata(file, chunkNum);
                Document doc = new Document(
                        UUID.randomUUID().toString(),
                        content.toString(),
                        metadata
                );
                documents.add(doc);
            }
        }

        return documents;
    }

    /**
     * 创建基础元数据
     */
    private Map<String, Object> createBaseMetadata(MultipartFile file, int sequence) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("source", file.getOriginalFilename());
        metadata.put("sequence", sequence);
        metadata.put("upload_time", System.currentTimeMillis());
        metadata.put("file_size", file.getSize());
        metadata.put("content_type", file.getContentType());
        return metadata;
    }

    /**
     * 获取单元格值作为字符串
     */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";

        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }

    /**
     * 获取文件扩展名
     */
    private String getFileExtension(String fileName) {
        if (fileName == null || fileName.lastIndexOf(".") == -1) {
            return "";
        }
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }

    /**
     * 分批存储到向量库
     */
    private List<String> storeInBatches(List<Document> documents, String fileName) {
        int totalProcessed = 0;
        int batchNumber = 0;
        List<String> vectorIds = new ArrayList<>();

        for (int i = 0; i < documents.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, documents.size());
            List<Document> batch = documents.subList(i, end);
            batchNumber++;

            int retryCount = 0;
            int maxRetries = 3;
            boolean success = false;

            while (!success && retryCount < maxRetries) {
                try {
                    vectorStore.add(batch);
                    for (Document doc : batch) {
                        vectorIds.add(doc.getId());
                    }
                    success = true;
                    totalProcessed += batch.size();
                    log.info("批次 {} 处理成功，已处理 {} 条，文件: {}",
                            batchNumber, totalProcessed, fileName);
                } catch (Exception e) {
                    retryCount++;
                    if (retryCount >= maxRetries) {
                        log.error("批次 {} 处理失败，已重试 {} 次", batchNumber, maxRetries, e);
                        throw new RuntimeException("批次 " + batchNumber + " 处理失败", e);
                    }
                    log.warn("批次 {} 处理失败，正在进行第 {} 次重试...", batchNumber, retryCount);
                    try {
                        Thread.sleep(1000 * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("重试被中断", ie);
                    }
                }
            }
        }
        return vectorIds;
    }

    /**
     * 长文本切分
     */
    private List<Document> splitLongText(String text, Map<String, Object> baseMetadata, int baseSequence) {
        List<Document> chunks = new ArrayList<>();
        int chunkSize = 1000; // 每块字符数
        int overlap = 200;     // 重叠字符数

        int start = 0;
        int chunkNum = 1;

        while (start < text.length()) {
            int end = Math.min(start + chunkSize, text.length());
            String chunk = text.substring(start, end);

            Map<String, Object> metadata = new HashMap<>(baseMetadata);
            metadata.put("chunk_number", chunkNum);
            metadata.put("chunk_start", start);
            metadata.put("chunk_end", end);

            Document doc = new Document(
                    UUID.randomUUID().toString(),
                    chunk,
                    metadata
            );
            chunks.add(doc);

            start += (chunkSize - overlap);
            chunkNum++;
        }

        return chunks;
    }
}
