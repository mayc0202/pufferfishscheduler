package com.pufferfishscheduler.ai.agent.controller;

import com.pufferfishscheduler.ai.agent.service.AgentService;
import com.pufferfishscheduler.common.result.ApiResponse;
import org.springframework.ai.chat.messages.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * Agent Controller
 *
 * @author Mayc
 * @since 2026-02-24  13:42
 */
@RestController
@RequestMapping("/agent")
public class AgentController {

    @Autowired
    private AgentService agentService;

    /**
     * 询问智能体
     *
     * @param question
     * @return
     */
    @GetMapping(value = "/askAgent", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> askAgent(@RequestParam("chatId") String chatId, @RequestParam("question") String question) {
        return agentService.askAgent(chatId, question);
    }

    /**
     * 获取历史消息列表
     *
     * @param chatId
     * @return
     */
    @GetMapping("/history")
    public List<Message> messages(@RequestParam("chatId") String chatId) {
        return agentService.getHistory(chatId);
    }

    /**
     * 清空历史消息
     *
     * @param chatId
     * @return
     */
    @GetMapping("/clearHistory")
    public ApiResponse clearHistory(@RequestParam("chatId") String chatId) {
        agentService.clearHistory(chatId);
        return ApiResponse.success("清空历史消息成功!");
    }
}