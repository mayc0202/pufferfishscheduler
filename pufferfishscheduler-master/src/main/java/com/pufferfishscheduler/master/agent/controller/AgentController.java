package com.pufferfishscheduler.master.agent.controller;

import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.master.agent.service.AIService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.ai.chat.messages.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * 智能体控制器
 *
 * @author Mayc
 * @since 2026-03-03  10:39
 */
@Tag(name = "智能体管理")
@RestController
@RequestMapping(value = "/agent")
public class AgentController {

    @Autowired
    private AIService aiService;

    /**
     * 询问智能体
     *
     * @param question
     * @return
     */
    @Operation(summary = "询问智能体")
    @GetMapping(value = "/askAgent.do", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> askAgent(@RequestParam("chatId") String chatId, @RequestParam("question") String question) {
        return aiService.askAgent(chatId, question);
    }

    /**
     * 获取历史消息列表
     *
     * @param chatId
     * @return
     */
    @Operation(summary = "获取历史消息列表")
    @GetMapping("/history.do")
    public List<Message> messages(@RequestParam("chatId") String chatId) {
        return aiService.getHistory(chatId);
    }

    /**
     * 清空历史消息
     *
     * @param chatId
     * @return
     */
    @Operation(summary = "清空历史消息")
    @GetMapping("/clearHistory")
    public ApiResponse clearHistory(@RequestParam("chatId") String chatId) {
        aiService.clearHistory(chatId);
        return ApiResponse.success("清空历史消息成功!");
    }

    /**
     * 文件切片
     *
     * @param file
     * @return
     */
    @Operation(summary = "文件切片")
    @PostMapping("/fileChunk.do")
    public ApiResponse fileChunk(@RequestParam("file") MultipartFile file) {
        aiService.fileChunk(file);
        return ApiResponse.success("文件切片成功!");
    }
}
