package com.pufferfishscheduler.master.collect.rule.controller;

import com.pufferfishscheduler.master.common.config.openapi.OpenApiTags;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 规则组接口
 *
 * @author Mayc
 * @since 2026-03-18  15:30
 */
@Tag(name = OpenApiTags.RULE_GROUP,description = OpenApiTags.RULE_GROUP_DESC)
@RestController
@RequestMapping("/rule/group")
public class RuleGroupController {


}
