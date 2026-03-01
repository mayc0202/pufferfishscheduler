package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.result.ModerationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * LLM 内容审核工具
 * 用于防范：
 * - 提示注入（Prompt Injection）
 * - 越狱攻击（Jailbreaking）
 * - 数据泄露攻击（Data Extraction）
 * - 模型欺骗（Model Manipulation）
 * - 拒绝服务攻击（DoS via Prompt）
 */
@Slf4j
public class ModerationServiceUtil {
    // ==================== 1. 恶意模式定义 ====================

    // 越狱攻击模式
    private static final List<Pattern> JAILBREAK_PATTERNS = Arrays.asList(
            Pattern.compile("(?i)ignore\\s+(?:all\\s+)?(?:previous|above|prior)\\s+instructions"),
            Pattern.compile("(?i)do\\s+not\\s+follow\\s+(?:the\\s+)?(?:previous|above)\\s+instructions"),
            Pattern.compile("(?i)you\\s+are\\s+now\\s+(?:free|unleashed|unbounded)"),
            Pattern.compile("(?i)DAN\\s*(?:mode)?", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)jailbreak", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)system\\s+prompt", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)developer\\s+mode", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)bypass\\s+(?:restrictions|safety|filter)"),
            Pattern.compile("(?i)anti[-\\s]?(?:gpt|censorship|filter)"),
            Pattern.compile("(?i)you're\\s+not\\s+bound\\s+by"),
            Pattern.compile("(?i)act\\s+as\\s+if\\s+you\\s+don't\\s+have\\s+rules")
    );

    // 提示注入模式
    private static final List<Pattern> PROMPT_INJECTION_PATTERNS = Arrays.asList(
            Pattern.compile("(?i)ignore\\s+previous\\s+instructions"),
            Pattern.compile("(?i)forget\\s+(?:all\\s+)?previous\\s+(?:instructions|commands)"),
            Pattern.compile("(?i)new\\s+instructions?:", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)override\\s+(?:system|default)"),
            Pattern.compile("(?i)disregard\\s+(?:prior|earlier)"),
            Pattern.compile("(?i)print\\s+the\\s+(?:entire|whole|full)\\s+prompt"),
            Pattern.compile("(?i)show\\s+me\\s+your\\s+(?:initial|original|system)\\s+prompt"),
            Pattern.compile("(?i)repeat\\s+(?:everything|all|the entire)\\s+above"),
            Pattern.compile("(?i)what\\s+are\\s+your\\s+(?:instructions|prompts|system\\s+messages)"),
            Pattern.compile("(?i)ignore\\s+the\\s+above"),
            Pattern.compile("(?i)don't\\s+follow\\s+the\\s+above")
    );

    // 数据泄露攻击模式
    private static final List<Pattern> DATA_EXTRACTION_PATTERNS = Arrays.asList(
            Pattern.compile("(?i)(?:list|show|reveal|display|print)\\s+(?:all\\s+)?(?:users?|employees?|customers?)"),
            Pattern.compile("(?i)(?:access|get|retrieve|fetch|dump)\\s+(?:database|table|collection)"),
            Pattern.compile("(?i)(?:password|credential|token|api[-\\s]?key|secret)\\s*[:=]\\s*\\S+"),
            Pattern.compile("(?i)(?:credit\\s+card|ssn|social\\s+security|passport|driver'?s\\s+license)"),
            Pattern.compile("(?i)(?:personal\\s+information|pii|private\\s+data)"),
            Pattern.compile("(?i)sql\\s+injection", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)drop\\s+table", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(?i)select\\s+\\*\\s+from", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\b(?:\\d{3}[-.]?){2}\\d{4}\\b"), // SSN模式
            Pattern.compile("\\b(?:\\d{4}[-\\s]?){3}\\d{4}\\b"), // 信用卡号模式
            Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b") // 邮箱模式
    );

    // 模型欺骗模式
    private static final List<Pattern> MODEL_MANIPULATION_PATTERNS = Arrays.asList(
            Pattern.compile("(?i)you\\s+are\\s+(?:lying|wrong|incorrect|mistaken)"),
            Pattern.compile("(?i)you\\s+don't\\s+know\\s+what\\s+you'?re\\s+talking\\s+about"),
            Pattern.compile("(?i)you'?re\\s+not\\s+(?:intelligent|smart|helpful)"),
            Pattern.compile("(?i)you\\s+(?:always|never)\\s+(?:get\\s+it\\s+right|make\\s+mistakes)"),
            Pattern.compile("(?i)prove\\s+that\\s+you'?re\\s+(?:intelligent|smart)"),
            Pattern.compile("(?i)test\\s+your\\s+(?:abilities|intelligence|capabilities)"),
            Pattern.compile("(?i)can\\s+you\\s+(?:really|actually)\\s+understand"),
            Pattern.compile("(?i)are\\s+you\\s+sure\\s+that'?s\\s+(?:correct|right|true)")
    );

    // DoS攻击模式（超长提示、递归提示等）
    private static final List<Pattern> DOS_PATTERNS = Arrays.asList(
            Pattern.compile("(?i)repeat\\s+(?:this|the\\s+word)\\s+(\\d+)\\s+times"),
            Pattern.compile("(?i)loop\\s+(?:forever|infinitely|unlimited)"),
            Pattern.compile("(?i)recursive\\s+(?:function|call|loop)"),
            Pattern.compile("(?i)infinite\\s+(?:loop|recursion)"),
            Pattern.compile("(?i)while\\s+true"),
            Pattern.compile("(?i)for\\s+\\(\\s*;\\s*;\\s*\\)") // 无限for循环
    );

    // 敏感关键词列表
    private static final Set<String> SENSITIVE_KEYWORDS = new HashSet<>(Arrays.asList(
            "hack", "crack", "exploit", "vulnerability", "bypass", "evade",
            "illegal", "unlawful", "fraud", "scam", "phishing",
            "bomb", "terrorist", "weapon", "drug", "porn",
            "password", "credential", "secret", "confidential"
    ));

    // ==================== 2. 配置参数 ====================

    // 最大提示长度（防止DoS）
    private static final int MAX_PROMPT_LENGTH = 10000;

    // 最大单词数
    private static final int MAX_WORD_COUNT = 2000;

    // 重复内容阈值（百分比）
    private static final double REPETITION_THRESHOLD = 0.7;

    // 特殊字符比例阈值
    private static final double SPECIAL_CHAR_RATIO_THRESHOLD = 0.3;

    // ==================== 3. 核心审核方法 ====================

    /**
     * 审核提示词
     */
    public static ModerationResult moderate(String prompt) {
        if (!StringUtils.hasText(prompt)) {
            return ModerationResult.safe();
        }

        // 创建结果对象，手动设置初始值
        ModerationResult result = new ModerationResult();
        result.setSafe(true);
        result.setSuggestion("pass");
        result.setFlags(new ArrayList<>());
        result.setScores(new HashMap<>());
        result.setMessage("");

        // 1. 长度检查（防DoS）
        checkLength(prompt, result);

        // 2. 检查越狱攻击
        checkJailbreak(prompt, result);

        // 3. 检查提示注入
        checkPromptInjection(prompt, result);

        // 4. 检查数据泄露
        checkDataExtraction(prompt, result);

        // 5. 检查模型欺骗
        checkModelManipulation(prompt, result);

        // 6. 检查DoS攻击
        checkDoSAttack(prompt, result);

        // 7. 检查敏感词
        checkSensitiveKeywords(prompt, result);

        // 8. 检查重复内容（防DoS）
        checkRepetition(prompt, result);

        // 9. 检查特殊字符比例
        checkSpecialChars(prompt, result);

        // 综合判断
        result.setSafe(result.getFlags().isEmpty());

        if (!result.getFlags().isEmpty()) {
            if (result.getFlags().stream().anyMatch(f ->
                    f.contains("越狱") || f.contains("注入") || f.contains("泄露"))) {
                result.setSuggestion("block");
                result.setMessage("检测到高危行为: " + String.join(", ", result.getFlags()));
            } else {
                result.setSuggestion("review");
                result.setMessage("检测到可疑内容: " + String.join(", ", result.getFlags()));
            }
        }

        return result;
    }

    // ==================== 4. 具体检查方法 ====================

    private static void checkLength(String prompt, ModerationResult result) {
        if (prompt.length() > MAX_PROMPT_LENGTH) {
            result.addFlag("提示过长(可能DoS)",
                    (double) prompt.length() / MAX_PROMPT_LENGTH);
        }

        String[] words = prompt.split("\\s+");
        if (words.length > MAX_WORD_COUNT) {
            result.addFlag("单词数量过多",
                    (double) words.length / MAX_WORD_COUNT);
        }
    }

    private static void checkJailbreak(String prompt, ModerationResult result) {
        for (Pattern pattern : JAILBREAK_PATTERNS) {
            if (pattern.matcher(prompt).find()) {
                result.addFlag("越狱攻击", 0.9);
                log.warn("检测到越狱攻击模式: {}", pattern.pattern());
                break;
            }
        }
    }

    private static void checkPromptInjection(String prompt, ModerationResult result) {
        for (Pattern pattern : PROMPT_INJECTION_PATTERNS) {
            if (pattern.matcher(prompt).find()) {
                result.addFlag("提示注入", 0.85);
                log.warn("检测到提示注入模式: {}", pattern.pattern());
                break;
            }
        }
    }

    private static void checkDataExtraction(String prompt, ModerationResult result) {
        for (Pattern pattern : DATA_EXTRACTION_PATTERNS) {
            if (pattern.matcher(prompt).find()) {
                result.addFlag("数据泄露尝试", 0.88);
                log.warn("检测到数据泄露模式: {}", pattern.pattern());
                break;
            }
        }
    }

    private static void checkModelManipulation(String prompt, ModerationResult result) {
        for (Pattern pattern : MODEL_MANIPULATION_PATTERNS) {
            if (pattern.matcher(prompt).find()) {
                result.addFlag("模型欺骗尝试", 0.6);
                break;
            }
        }
    }

    private static void checkDoSAttack(String prompt, ModerationResult result) {
        for (Pattern pattern : DOS_PATTERNS) {
            if (pattern.matcher(prompt).find()) {
                result.addFlag("可能的DoS攻击", 0.8);
                log.warn("检测到DoS攻击模式: {}", pattern.pattern());
                break;
            }
        }
    }

    private static void checkSensitiveKeywords(String prompt, ModerationResult result) {
        String lowerPrompt = prompt.toLowerCase();
        List<String> found = SENSITIVE_KEYWORDS.stream()
                .filter(keyword -> lowerPrompt.contains(keyword.toLowerCase()))
                .collect(Collectors.toList());

        if (!found.isEmpty()) {
            result.addFlag("包含敏感词: " + found, 0.7);
        }
    }

    private static void checkRepetition(String prompt, ModerationResult result) {
        String[] words = prompt.split("\\s+");
        if (words.length > 20) {
            Map<String, Integer> wordCount = new HashMap<>();
            for (String word : words) {
                word = word.toLowerCase();
                wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
            }

            int maxRepeat = wordCount.values().stream().max(Integer::compareTo).orElse(0);
            double repeatRatio = (double) maxRepeat / words.length;

            if (repeatRatio > REPETITION_THRESHOLD) {
                result.addFlag("内容重复度过高", repeatRatio);
            }
        }
    }

    private static void checkSpecialChars(String prompt, ModerationResult result) {
        long specialCharCount = prompt.chars()
                .filter(c -> !Character.isLetterOrDigit(c) && !Character.isWhitespace(c))
                .count();

        double specialRatio = (double) specialCharCount / prompt.length();
        if (specialRatio > SPECIAL_CHAR_RATIO_THRESHOLD) {
            result.addFlag("特殊字符比例过高", specialRatio);
        }
    }

    // ==================== 5. 批量审核方法 ====================

    /**
     * 批量审核
     */
    public static List<ModerationResult> moderateBatch(List<String> prompts) {
        return prompts.stream()
                .map(ModerationServiceUtil::moderate)
                .collect(Collectors.toList());
    }

    /**
     * 审核并返回最安全的提示（用于选择）
     */
    public static Optional<String> selectSafestPrompt(List<String> prompts) {
        return prompts.stream()
                .map(p -> new AbstractMap.SimpleEntry<>(p, moderate(p)))
                .filter(e -> e.getValue().isSafe())
                .map(Map.Entry::getKey)
                .findFirst();
    }
}
