package com.pufferfishscheduler.common.result;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Moderation 返回值
 *
 * @author Mayc
 * @since 2026-02-24  00:25
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ModerationResult {

    private boolean safe;
    private String suggestion; // pass, review, block
    private List<String> flags;
    private Map<String, Double> scores;
    private String message;

    // 手动添加添加flag的方法
    public void addFlag(String flag, double score) {
        if (this.flags == null) {
            this.flags = new ArrayList<>();
        }
        if (this.scores == null) {
            this.scores = new HashMap<>();
        }
        this.flags.add(flag);
        this.scores.put(flag, score);
    }

    public static ModerationResult safe() {
        return ModerationResult.builder()
                .safe(true)
                .suggestion("pass")
                .flags(new ArrayList<>())
                .scores(new HashMap<>())
                .message("内容安全")
                .build();
    }

    public static ModerationResult block(String reason) {
        ModerationResult result = ModerationResult.builder()
                .safe(false)
                .suggestion("block")
                .flags(new ArrayList<>())
                .scores(new HashMap<>())
                .message("内容被拦截: " + reason)
                .build();
        result.addFlag(reason, 1.0);
        return result;
    }

    public static ModerationResult review(String reason) {
        ModerationResult result = ModerationResult.builder()
                .safe(false)
                .suggestion("review")
                .flags(new ArrayList<>())
                .scores(new HashMap<>())
                .message("需要人工审核: " + reason)
                .build();
        result.addFlag(reason, 0.5);
        return result;
    }
}
