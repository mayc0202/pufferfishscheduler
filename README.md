# PufferfishScheduler

一款功能强大的数据采集调度器，基于Pentaho Kettle实现数据转换和处理。

## 项目简介

PufferfishScheduler是一个专为企业级数据采集和处理设计的调度系统，它提供了直观的可视化界面，支持复杂的数据转换流程设计和调度管理。系统基于Spring Boot构建，集成了Pentaho Kettle作为核心数据转换引擎，支持多种数据源的连接和处理。

## 系统架构

### 核心组件

1. **Master节点**：负责任务调度、流程管理和监控
2. **Worker节点**：负责执行具体的数据采集和处理任务
3. **数据转换引擎**：基于Pentaho Kettle实现，支持复杂的数据转换流程
4. **存储层**：使用MySQL存储任务配置和执行状态
5. **Web界面**：提供可视化的流程设计和任务管理界面

### 主要流程

1. 用户通过Web界面设计数据转换流程
2. 系统将流程配置保存到数据库
3. 调度器根据配置的触发条件启动任务
4. Worker节点执行数据转换流程
5. 执行结果被记录和监控

## 核心功能

- **可视化流程设计**：通过Web界面拖拽式设计数据转换流程
- **多数据源支持**：支持关系型数据库、文件、API等多种数据源
- **灵活的调度策略**：支持定时调度、事件触发等多种调度方式
- **实时监控**：实时监控任务执行状态和日志
- **错误处理**：完善的错误处理和重试机制
- **扩展性**：支持自定义步骤和插件

## 技术栈

- **后端**：Spring Boot, MyBatis-Plus
- **前端**：Vue.js (猜测，具体技术栈需根据实际情况调整)
- **数据转换**：Pentaho Kettle 9.2
- **存储**：MySQL
- **部署**：Docker (可选)

## 快速开始

### 环境要求

- JDK 11+
- Maven 3.6+
- MySQL 5.7+

### 安装步骤

1. **克隆项目**

```bash
git clone https://github.com/yourusername/pufferfishscheduler.git
cd pufferfishscheduler
```

2. **配置数据库**

创建MySQL数据库，执行`sql`目录下的初始化脚本。

3. **修改配置**

编辑`application.yml`文件，配置数据库连接信息。

4. **构建项目**

```bash
mvn clean package -DskipTests
```

5. **启动服务**

```bash
java -jar pufferfishscheduler-master/target/pufferfishscheduler-master-1.0.0.war
```

6. **访问界面**

打开浏览器，访问 `http://localhost:8080`，使用默认账号密码登录。

## 配置说明

### 核心配置文件

- `application.yml`：主配置文件，包含数据库连接、服务端口等配置
- `kettle-password-encoder-plugins.xml`：Kettle密码编码器配置

### Kettle插件配置

系统会自动加载`plugins`目录下的Kettle插件，可根据需要添加自定义插件。

## 项目结构

```
pufferfishscheduler/
├── pufferfishscheduler-domain/     # 领域模型
├── pufferfishscheduler-common/     # 通用工具类
├── pufferfishscheduler-dao/        # 数据访问层
├── pufferfishscheduler-api/        # API接口
├── pufferfishscheduler-master/     # 主节点
│   ├── src/main/java/com/pufferfishscheduler/master/collect/trans/engine/  # 数据转换引擎
│   └── src/main/resources/         # 资源文件
├── pufferfishscheduler-worker/     # 工作节点
└── pufferfishscheduler-ai/         # AI相关功能
```

## 开发指南

### 数据转换引擎

核心类包括：

- `DataTransEngine`：数据转换引擎核心类，负责Kettle环境初始化和转换执行
- `DataFlowRepository`：数据流程配置仓库，管理转换流程配置
- `DataCache`：数据缓存，提高转换执行效率
- `TransFlowServiceImpl`：转换流服务实现，处理转换流程的CRUD和执行

### 流程设计

1. 通过Web界面设计转换流程
2. 系统将流程配置保存到数据库
3. 执行时，系统加载配置并通过Kettle引擎执行转换

## 部署说明

### 单机部署

直接运行生成的WAR包即可。

### 集群部署

1. 配置多个Worker节点
2. 在Master节点配置Worker节点信息
3. 启动所有节点，Master节点会自动分配任务

## 常见问题

### Kettle初始化失败

- 检查`kettle-password-encoder-plugins.xml`配置是否正确
- 确保Kettle依赖包已正确加载

### 任务执行失败

- 检查数据源连接配置
- 查看执行日志，定位具体错误原因
- 检查转换流程设计是否正确

## 贡献指南

欢迎提交Issue和Pull Request，共同改进PufferfishScheduler。

## 许可证

[MIT License](LICENSE)

