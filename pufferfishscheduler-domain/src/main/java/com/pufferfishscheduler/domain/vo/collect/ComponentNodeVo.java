package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 组件菜单
 *
 * @author Mayc
 * @since 2026-03-02  23:21
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ComponentNodeVo {

    /**
     * 节点ID
     */
    private Integer id;

    /**
     * 节点标签（显示名称）
     */
    private String label;

    /**
     * 组件编码
     */
    private String code;

    /**
     * 节点类型：menu-菜单目录，plugin-插件
     */
    private String nodeType;

    /**
     * 组件类型编码（1-输入，2-输出，3-清洗，4-流程，5-脚本，6-文件传输，7-其它）
     */
    private String componentType;

    /**
     * 配置信息（JSON格式）
     */
    private String config;

    /**
     * 图标Base64
     */
    private String icon;

    /**
     * 节点宽度（仅插件类型使用）
     */
    private Integer width;

    /**
     * 节点高度（仅插件类型使用）
     */
    private Integer height;

    /**
     * 是否支持输入
     */
    private Boolean supportInput;

    /**
     * 是否支持错误处理
     */
    private Boolean supportError;

    /**
     * 是否支持复制
     */
    private Boolean supportCopy;

    /**
     * 是否支持本地文件
     */
    private Boolean supportLocalFile;

    /**
     * 阶段
     */
    private String stage;

    /**
     * 是否启用
     */
    private Boolean enabled;

    /**
     * 子节点列表（仅菜单类型有值）
     */
    private List<ComponentNodeVo> children;
}
