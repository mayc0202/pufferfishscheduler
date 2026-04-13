# PufferfishScheduler 插件生成技能指南

## 技能描述
当用户提供组件的 **JSON 配置结构**（参数定义）和 **功能描述**（业务逻辑）时，自动生成一个完整的 Kettle 转换步骤（Step）插件，并集成到 `pufferfishscheduler` 主工程中。生成的插件遵循项目现有规范（如 `pfs-debeziumjson-plugin`），支持单元测试验证。

## 触发条件
用户明确要求“生成 Kettle 组件/插件”，并提供：
- 组件的 JSON 配置示例（参数映射）
- 组件的功能描述（数据处理逻辑）
- （可选）额外依赖的 Maven 坐标

## 参考项目结构
- 生成模块：`pufferfishscheduler-plugin`
- 插件类路径：`com.pufferfishscheduler.plugin`
- 参考组件工程：`pfs-dataclean-plugin`、`pfs-debeziumjson-plugin`、`pfs-ftpupload-plugin`

## 输入格式示例
```json
{
  "componentName": "ExampleComponent",    // 驼峰，首字母大写
  "componentCode": "example_component",   // 小写+下划线，用于 StepMetaType 常量
  "version": "1.0.0",
  "jsonConfig": {
    "name": "组件显示名称",
    "data": {
      "sourceField": "json_field",        // 用户自定义参数
      "outputFields": [
        { "name": "field1", "type": "string" }
      ]
    }
  },
  "description": "从输入行的 JSON 字段中提取指定路径的值，并输出为行。",
  "dependencies": [                       // 可选
    "com.example:special-lib:1.0.0"
  ]
}
```

## 输出内容概览
### 1.插件模块工程（位于 pufferfishscheduler-plugin/ 下）
#### pom.xml（基于父模块，Kettle 依赖 scope=provided）
#### 三个核心 Java 文件：
```
{ComponentName}Step.java —— 执行逻辑
{ComponentName}StepData.java —— 运行时数据
{ComponentName}StepMeta.java —— 元数据（XML 序列化、参数 get/set、字段生成）

```
#### 可选文件：
```
{ComponentName}Utils.java（工具类）
{component-name}-config.yml（配置文件）
```
#### 资源文件：
```
src/main/resources/plugin.xml（插件描述，图标可忽略）
src/main/resources/version.xml（版本信息）
src/main/resources/{component-name}.svg（占位图标）
src/main/resources/assembly/assembly.xml（打包配置）
```

### 2.Master 工程集成（生成代码片段或 diff）
pufferfishscheduler-master/pom.xml 添加依赖

pufferfishscheduler-common/.../Constants.java 的 StepMetaType 接口添加常量

StepMetaConstructorFactory.java 添加 case

新建 {ComponentName}Constructor.java（继承 AbstractStepMetaConstructor）

### 3.部署目录（Master 的 resources/plugins/ 下）

目录名：pfs-{componentName}-plugin-{version}

包含：lib/（打包后的 jar）、plugin.xml、version.xml、图标

单元测试（可选但推荐）

## 详细生成规范
### 1. 插件模块结构（参考 pfs-debeziumjson-plugin）
#### 1.1 模块位置与命名
父模块：pufferfishscheduler-plugin（已存在）

子模块目录：pfs-{lowercase(componentName)}-plugin

artifactId：pfs-{lowercase(componentName)}-plugin

版本：与用户输入一致，默认 1.0.0

#### 1.2 子模块 pom.xml 模板
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.pufferfishscheduler.plugin</groupId>
        <artifactId>pufferfishscheduler-plugin</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <artifactId>pfs-${lowercaseComponentName}-plugin</artifactId>
    <version>${userVersion}</version>

    <dependencies>
        <!-- 所有 Kettle 核心依赖必须为 provided（由宿主提供） -->
        <dependency>
            <groupId>pentaho-kettle</groupId>
            <artifactId>kettle-core</artifactId>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>pentaho-kettle</groupId>
            <artifactId>kettle-engine</artifactId>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.pentaho</groupId>
            <artifactId>pentaho-metadata</artifactId>
            <scope>provided</scope>
        </dependency>
        <!-- SWT 由父模块管理，scope=provided -->
        <dependency>
            <groupId>org.eclipse.platform</groupId>
            <artifactId>org.eclipse.swt.win32.win32.x86_64</artifactId>
            <scope>provided</scope>
        </dependency>
        <!-- 用户额外依赖（如有） -->
        <!-- ... -->
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <release>17</release>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <executions>
                    <execution>
                        <id>distro-assembly</id>
                        <phase>package</phase>
                        <goals><goal>single</goal></goals>
                        <configuration>
                            <appendAssemblyId>false</appendAssemblyId>
                            <descriptors>
                                <descriptor>src/main/resources/assembly/assembly.xml</descriptor>
                            </descriptors>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
#### 1.3 三个核心 Java 文件
##### a. {ComponentName}StepMeta.java

注解：@Step(id = "{componentCode}", name = "显示名称", description = "描述", categoryDescription = "转换")

必须实现：
```
setDefault()

getXML() / loadXML(Node, ...)

saveRep() / readRep()

getFields(RowMetaInterface, ...) —— 声明输出字段

clone()

getStep() 返回 {ComponentName}Step

getStepData() 返回 {ComponentName}StepData
```
###### 注意：所有用户配置参数（来自 JSON）均需有私有字段、getter/setter，并在 XML 序列化中处理。参考 DebeziumJsonStepMeta 实现字段配置数组的存储方式。

##### b. {ComponentName}Step.java
继承 BaseStep 并实现 StepInterface

核心方法：
```
init() —— 解析配置，初始化输出字段映射

processRow() —— 获取输入行，执行业务逻辑，输出行

dispose() —— 释放资源

支持动态输出字段（在 processRow 中根据配置构建输出行）。
```

##### c. {ComponentName}StepData.java
继承 BaseStepData 并实现 StepDataInterface

通常只需定义 public RowMetaInterface outputRowMeta; 及运行时缓存。

#### 1.4 资源文件
plugin.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<plugin id="{componentCode}" type="Step">
  <name>组件显示名称</name>
  <description>组件描述</description>
  <category>转换</category>
  <classname>com.pufferfishscheduler.plugin.{ComponentName}StepMeta</classname>
  <tr>{component-name}.svg</image>
  <libraries>
    <library name="lib/pfs-${lowercaseComponentName}-plugin-${version}.jar"/>
  </libraries>
</plugin>
```
version.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<version branch='TRUNK'>${project.version}</version>
```
assembly.xml（保持与 pfs-debeziumjson-plugin 一致，仅需排除 Kettle 核心包）
图标文件：可生成一个空的占位 SVG 或使用默认图标，内容不限。

### 2. Master 工程集成步骤
#### 2.1 添加模块依赖
在 pufferfishscheduler-master/pom.xml 的 <dependencies> 中添加：
```xml
<dependency>
    <groupId>com.pufferfishscheduler.plugin</groupId>
    <artifactId>pfs-${lowercaseComponentName}-plugin</artifactId>
    <version>${userVersion}</version>
</dependency>
```

#### 2.2 在 Constants.java 的 StepMetaType 接口中添加常量
```java
String {COMPONENT_NAME_UPPER} = "{componentCode}";
```

#### 2.3 在 StepMetaConstructorFactory.java 的 switch 中添加 case
```java
case Constants.StepMetaType.{COMPONENT_NAME_UPPER}:
return new {ComponentName}Constructor();
```

#### 2.4 创建构造器 {ComponentName}Constructor.java
包路径：com.pufferfishscheduler.master.collect.trans.plugin.constructor

继承 AbstractStepMetaConstructor

实现 create(String config, TransMeta transMeta, StepContext context) 方法

解析 JSON 配置（用户传入的 jsonConfig），设置到 {ComponentName}StepMeta 实例

使用 StepPluginType.class 获取 pluginId

处理 copiesCache、distributeType 等通用字段

如果组件需要前置/后置步骤，重写 beforeStep() / afterStep()

构造器代码模板（参考 DebeziumJsonConstructor）：

```java
public class {ComponentName}Constructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        validateInput(config, context);
        JSONObject jsonObject = JSONObject.parseObject(config);
        // 提取 name, data
        String name = jsonObject.getString("name");
        JSONObject data = jsonObject.getJSONObject("data");
        {ComponentName}StepMeta meta = new {ComponentName}StepMeta();
        meta.setDefault();
        // 从 data 中读取参数并设置到 meta
        // ...
        String pluginId = context.getRegistryID().getPluginId(StepPluginType.class, meta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (stepMeta == null) {
            stepMeta = new StepMeta(pluginId, name, meta);
        } else {
            stepMeta.setStepID(pluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(meta);
        }
        // 处理 copies, distribute 等
        return stepMeta;
    }
}
```

#### 2.5 部署目录生成
在 pufferfishscheduler-master/src/main/resources/plugins/ 下创建目录：

```text
pfs-{lowercaseComponentName}-plugin-{version}/
├── lib/
│   └── pfs-{lowercaseComponentName}-plugin-{version}.jar
├── plugin.xml
├── version.xml
└── {component-name}.svg
```
###### 注意：此目录的内容会在 Maven 构建时通过 maven-antrun-plugin 复制到 target/classes/plugins/steps/ 下供 Kettle 扫描。你需要生成该目录及其文件，并建议在 SKILL 中指导开发者手动复制或通过后续脚本处理。由于 Agent 无法直接操作文件系统，应输出完整的目录结构和文件内容，由开发者放置。

#### Agent 应输出：

1.完整的 Maven 模块 pfs-jsonextractor-plugin 的所有文件（pom.xml, JsonExtractorStep.java, JsonExtractorStepData.java, JsonExtractorStepMeta.java, plugin.xml, version.xml, assembly.xml, 占位图标）。

2.Master 工程中需要修改/新增的代码片段（pom.xml 依赖、Constants.java 常量、工厂类 case、JsonExtractorConstructor.java）。

3.部署目录结构 pfs-jsonextractor-plugin-1.0.0/ 及其内容。

4.简要集成说明。

## 约束与注意事项
1.不生成实际图标文件：提供占位说明即可，前端图标由数据库存储的路径决定。

2.依赖管理：所有 Kettle 核心依赖必须 <scope>provided</scope>，用户额外依赖可设为 compile（但注意避免与宿主冲突）。

3.前后置步骤：只有用户功能描述中明确需要（如连接初始化、资源释放）时才重写 beforeStep/afterStep。

4.代码风格：遵循项目现有风格（Lombok 可选，日志使用 log 对象）。

## 交付物清单
1.插件模块所有源代码文件（直接可复制）

2.Master 工程修改的 diff 或完整代码块

3.部署目录的完整文件列表及内容

4.简短集成指南（编译、部署、验证步骤）



