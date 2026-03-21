package com.pufferfishscheduler.master.collect.trans.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.result.ApiResponse;
import com.pufferfishscheduler.domain.form.collect.PreviewForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowConfigForm;
import com.pufferfishscheduler.domain.form.collect.TransFlowForm;
import com.pufferfishscheduler.master.collect.trans.engine.DataTransEngine.ResourceType;
import com.pufferfishscheduler.master.collect.trans.engine.logchannel.LogChannel;
import com.pufferfishscheduler.master.collect.trans.engine.logchannel.LogChannelManager;
import com.pufferfishscheduler.master.collect.trans.service.TransFlowService;
import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

/**
 * 转换流控制层
 *
 * @author Mayc
 * @since 2026-03-04 17:44
 */
@Tag(name = OpenApiTags.TRANS_FLOW_PROCESS, description = OpenApiTags.TRANS_FLOW_PROCESS_DESC)
@Validated
@RestController
@RequestMapping(value = "/trans/flow", produces = { "application/json;charset=utf-8" })
public class TransFlowController {

    @Autowired
    private TransFlowService flowService;

    /**
     * 转换分组 + 转换流程树（用于左侧导航等）
     *
     * @param name 分组名称模糊查询，可选
     */
    @Operation(summary = "转换流程树（分组+流程）")
    @GetMapping("/tree.do")
    public ApiResponse tree(@RequestParam(required = false) String name) {
        return ApiResponse.success(flowService.tree(name));
    }

    /**
     * 查询转换流程列表
     *
     * @param groupId
     * @param flowName
     * @param pageNo
     * @param pageSize
     * @return 转换流程列表
     */
    @Operation(summary = "查询转换流程列表")
    @GetMapping("/list.do")
    public ApiResponse list(@RequestParam(required = false, name = "groupId") Integer groupId,
            @RequestParam(required = false, name = "flowName") String flowName,
            @RequestParam(defaultValue = "1") Integer pageNo,
            @RequestParam(defaultValue = "10") Integer pageSize) {
        return ApiResponse.success(flowService.list(groupId, flowName, pageNo, pageSize));
    }

    /**
     * 添加转换流程
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "添加转换流程")
    @PostMapping("/add.do")
    public ApiResponse add(@RequestBody @Valid TransFlowForm form) {
        flowService.addFlow(form, Constants.STAGE.CONVERGE);
        return ApiResponse.success("流程添加成功!");
    }

    /**
     * 保存转换流程配置
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "保存转换流程配置")
    @PostMapping("/setConfig.do")
    public ApiResponse setConfig(@RequestBody @Valid TransFlowConfigForm form) {
        flowService.setConfig(form);
        return ApiResponse.success("流程配置保存成功!");
    }

    /**
     * 获取转换流程详情
     *
     * @param id 流程id
     * @return 转换流程id
     */
    @Operation(summary = "获取转换流程详情")
    @GetMapping("/detail.do")
    public ApiResponse detail(@RequestParam(name = "id") Integer id) {
        return ApiResponse.success(flowService.detail(id));
    }

    /**
     * 编辑转换流程
     *
     * @param form 转换流程表单
     * @return 转换流程id
     */
    @Operation(summary = "编辑转换流程")
    @PutMapping("/update.do")
    public ApiResponse update(@RequestBody @Valid TransFlowForm form) {
        flowService.updateFlow(form);
        return ApiResponse.success("流程修改成功!");
    }

    /**
     * 删除转换流程
     *
     * @param id 转换流程id
     * @return 转换流程id
     */
    @Operation(summary = "删除转换流程")
    @PutMapping("/delete.do")
    public ApiResponse delete(@RequestParam(name = "") Integer id) {
        flowService.deleteFlow(id);
        return ApiResponse.success("流程删除成功!");
    }

    /**
     * 预览数据
     *
     * @param form
     * @return
     */
    @Operation(summary = "预览数据")
    @PostMapping(value = "/preview.do")
    public ApiResponse preview(@RequestBody @Valid PreviewForm form) {
        return ApiResponse.success(flowService.preview(form));
    }

    /**
     * 获取转换流字段流
     *
     * @param flowId 转换流id
     * @param config 转换流配置
     * @param stepName 转换流步骤名称
     * @param type 字段类型
     * @return 字段流
     */
    @Operation(summary = "获取转换流字段流")
    @GetMapping(value = "/getFieldStream.do")
    public ApiResponse getFieldStream(@RequestParam Integer flowId,
            @RequestParam String config,
            @RequestParam String stepName,
            @RequestParam Integer type) {
        return ApiResponse.success(flowService.getFieldStream(flowId, config, stepName, type));
    }

    /**
     * 执行转换流
     * 
     * @param id
     * @return
     */
    @Operation(summary = "执行转换流程")
    @GetMapping(value = "/execute.do")
    public ApiResponse execute(@RequestParam Integer id) {
        flowService.execute(id);
        return ApiResponse.success("转换流程启动成功！");
    }

    /**
     * 停止转换流程
     * 
     * @param id
     * @return
     */
    @Operation(summary = "停止转换流程")
    @GetMapping("/stop.do")
    public ApiResponse getConfig(@RequestParam Integer id) {
        flowService.stop(id);

        // 等待30秒，流程停止
        LogChannel channel = LogChannelManager.get(LogChannelManager.getKey(ResourceType.TRANS.name(), id + ""));
        if (channel == null) {
            return ApiResponse.failure("请校验当前流程是否运行!");
        }

        for (int i = 0; i < 60; i++) {
            if (Constants.EXECUTE_STATUS.SUCCESS.equals(channel.getStatus()) ||
                    Constants.EXECUTE_STATUS.FAILURE.equals(channel.getStatus())) {
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            }
        }

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        }

        if (Constants.EXECUTE_STATUS.SUCCESS.equals(channel.getStatus()) ||
                Constants.EXECUTE_STATUS.FAILURE.equals(channel.getStatus())) {
            return ApiResponse.success("转换流程停止成功！");
        } else {
            return ApiResponse.failure("转换流程停止失败，请稍后再试！");
        }
    }

    /**
     * 获取流程运行日志
     *
     * @param id
     * @return
     */
    @Operation(summary = "流程运行日志")
    @GetMapping(value = "/getProcessLog.do")
    public ApiResponse getProcessLog(@RequestParam Integer id) {
        LogChannel logChannel = flowService.getProcessLog(id);
        return ApiResponse.success(logChannel.clone());
    }

    /**
     * 校验流程是否执行
     *
     * @param id
     * @return
     */
    @Operation(summary = "校验转换流程运行状态")
    @GetMapping("/checkTransStatus.do")
    public ApiResponse checkTransStatus(@RequestParam Integer id) {
        return ApiResponse.success(flowService.checkTransStatus(id));
    }

    /**
     * 展示转换流图片
     * 
     * @param id
     * @return
     */
    @Operation(summary = "展示转换流图片")
    @GetMapping("/showTransImg.do")
    public ApiResponse showTransImg(@RequestParam Integer id) {
        return ApiResponse.success(flowService.showTransImg(id));
    }

     /**
      * 复制转换流
      * 
      * @param id
      * @return
      */
    @Operation(summary = "复制转换流")
    @PostMapping("/copy.do")
    public ApiResponse copy(@RequestParam Integer id) {
        flowService.copy(id);
        return ApiResponse.success("复制转换流程成功！");
    }
}
