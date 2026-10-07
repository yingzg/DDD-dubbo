# ddd-dubbo

DDD 增强的通用 **Dubbo 后端业务服务框架**。

## 定位

承载核心业务的后端领域服务框架：以 Dubbo Provider 为服务出口，用 DDD 五模块分层约束代码位置，内置 CmdExe/QryExe 用例执行器、扩展点机制、领域事件、状态机、统一异常与响应、上下文透传，以及数据访问、缓存锁、MQ、任务调度、线程池、可观测性等公共治理能力。业务方只专注写领域逻辑。

- **纯公共**：无任何公司内部私有依赖，全开源组件。
- **DDD 增强**：五模块分层 + CmdExe/QryExe 用例执行器 + 扩展点机制 + 领域事件 + 状态机 + 依赖倒置。
- **双进程**：在线服务（app）与任务服务（job）隔离，共享 domain/infra。

## 技术基线

| 维度 | 选型 |
|---|---|
| 语言 / 运行时 | Java 8 |
| 容器 | Spring Boot 2.7.x |
| 服务通信 | Dubbo 2.7.x（开源版） |
| 数据访问 | MyBatis + Druid |
| 缓存 / 锁 | Redis / Redisson + Caffeine |
| 消息队列 | RocketMQ |
| 任务调度 | XXL-Job |
| 配置中心 | Nacos |
| 可观测性 | OpenTelemetry（traceId / spanId） |

## 目录

```
docs/    框架详细设计文档
```

## 状态

框架详细设计进行中。代码实施由独立 agent 执行。
