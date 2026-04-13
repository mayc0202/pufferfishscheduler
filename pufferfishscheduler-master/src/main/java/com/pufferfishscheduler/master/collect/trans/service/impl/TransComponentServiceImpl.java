package com.pufferfishscheduler.master.collect.trans.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.enums.TransComponentType;
import com.pufferfishscheduler.dao.entity.TransComponent;
import com.pufferfishscheduler.dao.mapper.TransComponentMapper;
import com.pufferfishscheduler.domain.vo.collect.ComponentNodeVo;
import com.pufferfishscheduler.master.collect.trans.service.TransComponentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 转换组件服务实现类
 *
 * @author Mayc
 * @since 2026-03-02 22:49
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransComponentServiceImpl implements TransComponentService {

    private final TransComponentMapper transComponentMapper;

    /**
     * 插件节点默认宽度
     */
    private static final int DEFAULT_PLUGIN_WIDTH = 100;

    /**
     * 插件节点默认高度
     */
    private static final int DEFAULT_PLUGIN_HEIGHT = 40;

    /**
     * 获取组件树形结构
     *
     * @return 组件树节点列表
     */
    @Override
    public List<ComponentNodeVo> getComponentTree() {
        // 查询所有启用的组件，按类型和排序字段排序
        List<TransComponent> enabledComponents = queryEnabledComponents();

        if (CollectionUtils.isEmpty(enabledComponents)) {
            log.info("未找到启用的转换组件");
            return new ArrayList<>();
        }

        // 按组件类型分组
        Map<String, List<TransComponent>> typeGroupMap = groupByComponentType(enabledComponents);

        // 构建组件树
        return buildComponentTree(typeGroupMap);
    }

    /**
     * 查询启用的组件
     */
    private List<TransComponent> queryEnabledComponents() {
        LambdaQueryWrapper<TransComponent> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(TransComponent::getEnabled, true)
                .orderByAsc(TransComponent::getType)
                .orderByAsc(TransComponent::getOrderBy);

        return transComponentMapper.selectList(queryWrapper);
    }

    /**
     * 按组件类型分组
     */
    private Map<String, List<TransComponent>> groupByComponentType(List<TransComponent> components) {
        return components.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(
                        TransComponent::getType,
                        Collectors.mapping(comp -> comp, Collectors.toList())
                ));
    }

    /**
     * 构建组件树
     */
    private List<ComponentNodeVo> buildComponentTree(Map<String, List<TransComponent>> typeGroupMap) {
        List<ComponentNodeVo> treeNodes = new ArrayList<>();

        // 按枚举顺序遍历所有类型
        for (TransComponentType typeEnum : TransComponentType.values()) {
            String typeCode = typeEnum.getType();
            List<TransComponent> typeComponents = typeGroupMap.get(typeCode);

            if (CollectionUtils.isNotEmpty(typeComponents)) {
                // 构建菜单目录节点
                ComponentNodeVo catalogNode = buildCatalogNode(typeEnum, typeCode);
                // 构建子节点（插件列表）
                catalogNode.setChildren(buildPluginNodes(typeComponents));
                treeNodes.add(catalogNode);
            }
        }

        return treeNodes;
    }

    /**
     * 构建菜单目录节点
     */
    private ComponentNodeVo buildCatalogNode(TransComponentType typeEnum, String typeCode) {
        return ComponentNodeVo.builder()
                .id(Integer.valueOf(typeEnum.getType()))  // 使用类型编码作为目录节点ID
                .label(typeEnum.getDescription())
                .icon(typeEnum.getIcon())
                .nodeType(Constants.TREE_TYPE.MENU)
                .componentType(typeCode)
                .build();
    }

    /**
     * 构建插件节点列表
     */
    private List<ComponentNodeVo> buildPluginNodes(List<TransComponent> components) {
        return components.stream()
                .filter(Objects::nonNull)
                .map(this::convertToPluginNode)
                .collect(Collectors.toList());
    }

    /**
     * 将实体转换为插件节点
     */
    private ComponentNodeVo convertToPluginNode(TransComponent component) {
        ComponentNodeVo.ComponentNodeVoBuilder builder = ComponentNodeVo.builder()
                .id(component.getId())
                .label(component.getName())
                .code(component.getCode())
                .componentType(component.getType())
                .icon(component.getIcon())
                .nodeType(Constants.TREE_TYPE.PLUGIN)
                .enabled(component.getEnabled())
                .config(component.getConfig())
                .width(DEFAULT_PLUGIN_WIDTH)
                .height(DEFAULT_PLUGIN_HEIGHT);

        // 设置可选属性
        if (component.getSupportInput() != null) {
            builder.supportInput(component.getSupportInput());
        }
        if (component.getSupportError() != null) {
            builder.supportError(component.getSupportError());
        }
        if (component.getSupportCopy() != null) {
            builder.supportCopy(component.getSupportCopy());
        }
        if (component.getSupportLocalFile() != null) {
            builder.supportLocalFile(component.getSupportLocalFile());
        }
        if (StringUtils.isNotBlank(component.getStage())) {
            builder.stage(component.getStage());
        }

        return builder.build();
    }
}