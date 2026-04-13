package com.pufferfishscheduler.master.dashboard.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.*;
import com.pufferfishscheduler.dao.mapper.*;
import com.pufferfishscheduler.domain.vo.dashboard.DashboardSummaryVo;
import com.pufferfishscheduler.master.dashboard.service.DashboardService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 首页仪表盘统计服务实现
 */
@Slf4j
@Service
public class DashboardServiceImpl implements DashboardService {

    private static final String MODULE_METADATA = "metadata";
    private static final String MODULE_TRANS = "trans";
    private static final String MODULE_CLEAN = "clean";
    private static final String MODULE_REALTIME = "realtime";

    @Autowired
    private MetadataTaskMapper metadataTaskMapper;

    @Autowired
    private TransTaskMapper transTaskMapper;

    @Autowired
    private TransFlowMapper transFlowMapper;

    @Autowired
    private RtTaskMapper rtTaskMapper;

    @Autowired
    private KbDocumentMapper kbDocumentMapper;

    @Autowired
    private KbDocumentAttachmentMapper kbDocumentAttachmentMapper;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public DashboardSummaryVo summary() {
        long metadataCount = countNotDeleted(metadataTaskMapper, "metadata_task");
        long transCount = countNotDeleted(transTaskMapper, "trans_task");
        long realtimeCount = countRtNotDeleted();
        long cleanCount = countCleanTasks();
        long total = metadataCount + transCount + realtimeCount + cleanCount;

        Map<String, DashboardSummaryVo.StatusDistribution> statusDistributions = new LinkedHashMap<>();
        statusDistributions.put(MODULE_METADATA, DashboardSummaryVo.StatusDistribution.builder()
                .counts(metadataStatusCounts())
                .build());
        statusDistributions.put(MODULE_TRANS, DashboardSummaryVo.StatusDistribution.builder()
                .counts(transStatusCounts(null))
                .build());
        statusDistributions.put(MODULE_REALTIME, DashboardSummaryVo.StatusDistribution.builder()
                .counts(rtStatusCounts())
                .build());
        statusDistributions.put(MODULE_CLEAN, DashboardSummaryVo.StatusDistribution.builder()
                .counts(transStatusCounts(getCleanFlowIds()))
                .build());

        Map<String, DashboardSummaryVo.Progress> progress = new LinkedHashMap<>();
        progress.put(MODULE_METADATA, computeProgress(metadataCount, statusDistributions.get(MODULE_METADATA).getCounts()));
        progress.put(MODULE_TRANS, computeProgress(transCount, statusDistributions.get(MODULE_TRANS).getCounts()));
        progress.put(MODULE_REALTIME, computeProgress(realtimeCount, statusDistributions.get(MODULE_REALTIME).getCounts()));
        progress.put(MODULE_CLEAN, computeProgress(cleanCount, statusDistributions.get(MODULE_CLEAN).getCounts()));

        DashboardSummaryVo.KnowledgeOverview knowledgeOverview = buildKnowledgeOverview();

        return DashboardSummaryVo.builder()
                .taskCards(DashboardSummaryVo.TaskCards.builder()
                        .totalTasks(total)
                        .metadataTasks(metadataCount)
                        .cleanTasks(cleanCount)
                        .realtimeTasks(realtimeCount)
                        .transTasks(transCount)
                        .build())
                .statusDistributions(statusDistributions)
                .progress(progress)
                .knowledgeOverview(knowledgeOverview)
                .build();
    }

    private DashboardSummaryVo.Progress computeProgress(long total, Map<String, Long> statusCounts) {
        long failure = statusCounts == null ? 0L : statusCounts.getOrDefault(Constants.JOB_MANAGE_STATUS.FAILURE, 0L);
        int percent;
        if (total <= 0) {
            percent = 0;
        } else {
            long ok = Math.max(0L, total - failure);
            percent = (int) Math.round((ok * 100.0d) / total);
        }
        percent = Math.max(0, Math.min(100, percent));
        return DashboardSummaryVo.Progress.builder()
                .percent(percent)
                .failureCount(failure)
                .totalCount(total)
                .build();
    }

    private DashboardSummaryVo.KnowledgeOverview buildKnowledgeOverview() {
        long knowledgeCount = kbDocumentMapper.selectCount(new LambdaQueryWrapper<KbDocument>()
                .eq(KbDocument::getDeleted, Constants.DELETE_FLAG.FALSE));

        long categoryCount = kbDocumentMapper.selectList(new QueryWrapper<KbDocument>()
                        .select("category_id")
                        .eq("deleted", Constants.DELETE_FLAG.FALSE)
                        .isNotNull("category_id"))
                .stream()
                .map(KbDocument::getCategoryId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet())
                .size();

        long relationCount = kbDocumentAttachmentMapper.selectCount(new LambdaQueryWrapper<KbDocumentAttachment>()
                .eq(KbDocumentAttachment::getDeleted, Constants.DELETE_FLAG.FALSE));

        long tagCount = countDistinctTags();

        return DashboardSummaryVo.KnowledgeOverview.builder()
                .knowledgeCount(knowledgeCount)
                .categoryCount(categoryCount)
                .relationCount(relationCount)
                .tagCount(tagCount)
                .build();
    }

    private long countDistinctTags() {
        List<KbDocument> docs = kbDocumentMapper.selectList(new QueryWrapper<KbDocument>()
                .select("tags_json")
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .isNotNull("tags_json"));
        if (CollectionUtils.isEmpty(docs)) {
            return 0L;
        }
        Set<String> tags = new HashSet<>();
        for (KbDocument doc : docs) {
            String json = doc == null ? null : doc.getTagsJson();
            if (StringUtils.isBlank(json)) {
                continue;
            }
            try {
                List<String> arr = objectMapper.readValue(json, new TypeReference<List<String>>() {});
                if (arr == null) continue;
                for (String t : arr) {
                    if (StringUtils.isNotBlank(t)) {
                        tags.add(t.trim());
                    }
                }
            } catch (Exception e) {
                // 标签字段为用户输入，容错：出现非JSON数组时不影响首页
                log.warn("解析 kb_document.tags_json 失败，已跳过。json={}", json);
            }
        }
        return tags.size();
    }

    private long countRtNotDeleted() {
        return rtTaskMapper.selectCount(new LambdaQueryWrapper<RtTask>()
                .eq(RtTask::getDeleted, Constants.DELETE_FLAG.FALSE));
    }

    private long countCleanTasks() {
        List<Integer> cleanFlowIds = getCleanFlowIds();
        if (CollectionUtils.isEmpty(cleanFlowIds)) {
            return 0L;
        }
        QueryWrapper<TransTask> qw = new QueryWrapper<>();
        qw.eq("deleted", Constants.DELETE_FLAG.FALSE)
                .in("flow_id", cleanFlowIds);
        return transTaskMapper.selectCount(qw);
    }

    private List<Integer> getCleanFlowIds() {
        List<TransFlow> flows = transFlowMapper.selectList(new LambdaQueryWrapper<TransFlow>()
                .eq(TransFlow::getDeleted, Constants.DELETE_FLAG.FALSE)
                .eq(TransFlow::getStage, Constants.STAGE.INTEGRATION));
        if (CollectionUtils.isEmpty(flows)) {
            return Collections.emptyList();
        }
        return flows.stream()
                .map(TransFlow::getId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }

    private long countNotDeleted(MetadataTaskMapper mapper, String ignored) {
        return mapper.selectCount(new LambdaQueryWrapper<MetadataTask>()
                .eq(MetadataTask::getDeleted, Constants.DELETE_FLAG.FALSE));
    }

    private long countNotDeleted(TransTaskMapper mapper, String ignored) {
        return mapper.selectCount(new LambdaQueryWrapper<TransTask>()
                .eq(TransTask::getDeleted, Constants.DELETE_FLAG.FALSE));
    }

    private Map<String, Long> metadataStatusCounts() {
        QueryWrapper<MetadataTask> qw = new QueryWrapper<>();
        qw.select("status", "count(1) as cnt")
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .groupBy("status");
        return toStatusCountMap(metadataTaskMapper.selectMaps(qw), "metadata_task");
    }

    private Map<String, Long> rtStatusCounts() {
        QueryWrapper<RtTask> qw = new QueryWrapper<>();
        qw.select("status", "count(1) as cnt")
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .groupBy("status");
        return toStatusCountMap(rtTaskMapper.selectMaps(qw), "rt_task");
    }

    private Map<String, Long> transStatusCounts(List<Integer> flowIds) {
        QueryWrapper<TransTask> qw = new QueryWrapper<>();
        qw.select("status", "count(1) as cnt")
                .eq("deleted", Constants.DELETE_FLAG.FALSE);
        if (!CollectionUtils.isEmpty(flowIds)) {
            qw.in("flow_id", flowIds);
        }
        qw.groupBy("status");
        return toStatusCountMap(transTaskMapper.selectMaps(qw), "trans_task");
    }

    private Map<String, Long> toStatusCountMap(List<Map<String, Object>> rows, String type) {
        try {
            if (CollectionUtils.isEmpty(rows)) {
                return Collections.emptyMap();
            }
            Map<String, Long> result = new LinkedHashMap<>();
            for (Map<String, Object> row : rows) {
                if (row == null) continue;
                Object statusObj = row.get("status");
                Object cntObj = row.get("cnt");
                String status = statusObj == null ? null : String.valueOf(statusObj);
                if (StringUtils.isBlank(status)) continue;
                long cnt = 0L;
                if (cntObj instanceof Number n) {
                    cnt = n.longValue();
                } else if (cntObj != null) {
                    try {
                        cnt = Long.parseLong(String.valueOf(cntObj));
                    } catch (Exception ignored) {
                        cnt = 0L;
                    }
                }
                result.put(status, cnt);
            }
            return result;
        } catch (Exception e) {
            log.warn("dashboard 统计状态分布失败，type={}", type, e);
            return Collections.emptyMap();
        }
    }
}

