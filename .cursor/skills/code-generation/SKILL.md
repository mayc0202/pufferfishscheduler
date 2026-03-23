# PufferfishScheduler 代码生成技能指南

## 技能描述
基于用户提供的数据库表结构，自动生成符合PufferfishScheduler项目规范的实体类、Mapper接口和Mapper XML文件，并生成到对应的目录。

## 参考项目结构
- 生成模块：`pufferfishscheduler-dao`
- 实体类路径：`com.pufferfishscheduler.dao.entity`
- Mapper接口路径：`com.pufferfishscheduler.dao.mapper`
- Mapper XML路径：`resources/mapper`

## 代码生成规则

### 1. 实体类生成规则
参考 `com.pufferfishscheduler.dao.entity` 下的已有实体类：  
生成规范：
```text
类名使用大驼峰命名（表名转类名：user_info → UserInfo）

必须添加 @Data 和 @TableName 注解

主键字段使用 @TableId 注解，自增主键使用 IdType.AUTO

日期类型使用 java.util.Date

字段名使用小驼峰命名（数据库字段转Java字段：user_name → userName）

自动填充字段添加 @TableField 注解
```
样例代码参考：
```java
package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.util.Date;

@Data
@TableName("表名")
public class 实体类名 {
    
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;
    
    private String fieldName;
    
    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date updateTime;
}
```
### 2. Mapper接口生成规则
参考 com.pufferfishscheduler.dao.mapper 下的Mapper接口：
生成规范：
```text
接口名 = 实体类名 + "Mapper"（表名转接口名：user_info → UserInfoMapper）

必须继承 BaseMapper<实体类名>

必须添加 @Mapper 注解

```
样例代码参考：
```java
package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.实体类名;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface 实体类名Mapper extends BaseMapper<实体类名> {

}

```
### 3. Mapper XML生成规则
参考 resources/mapper 下的Mapper XML文件：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.pufferfishscheduler.dao.mapper.实体类名Mapper">

    <!-- 通用查询映射结果 -->
    <resultMap id="BaseResultMap" type="com.pufferfishscheduler.dao.entity.实体类名">
        <id column="id" property="id" />
        <result column="字段名" property="字段名" />
        <!-- 其他字段映射 -->
    </resultMap>

    <!-- 通用查询结果列 -->
    <sql id="Base_Column_List">
        id, 字段1, 字段2, create_time, update_time
    </sql>

</mapper>
```






