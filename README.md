# ddd-dubbo

DDD 增强的通用 **Dubbo 后端业务服务框架**。

承载核心业务的后端领域服务骨架：以 Dubbo Provider 为服务出口，用 DDD 五模块分层约束代码位置，内置用例执行器、扩展点、状态机、领域事件、Outbox 最终一致等 DDD/COLA 机制，以及统一异常、上下文透传、数据访问、缓存锁、MQ、任务调度、线程池、可观测性等公共治理能力。业务方只专注写领域逻辑。

- **纯公共**：零公司内部私有依赖，全开源组件。
- **DDD 增强**：五模块分层 + 依赖倒置 + CmdExe/QryExe + 扩展点 + 状态机 + 领域事件。
- **双进程**：在线服务（app）与任务服务（job）隔离，共享 domain/infra。

## 核心特性（基础功能点）

### DDD 分层与用例

| 能力 | 说明 |
|---|---|
| 五模块分层 | `api`（RPC 契约，零依赖）/ `domain`（领域 + 用例执行器）/ `infra`（基础设施实现）/ `app`（在线服务）/ `job`（任务进程），严格依赖倒置（domain 定义接口，infra 实现） |
| CmdExe / QryExe 用例执行器 | CQRS 命令/查询分离，CommandBus/QueryBus 通过泛型自动路由到匹配执行器 |
| 扩展点机制 | ExtensionPoint + `@Extension` + BizScenario（bizId+useCase+scenario）三级路由，替代 if-else |
| 无状态状态机 | StateMachine + Builder 声明式定义「状态+事件→目标状态+动作」，`fireEvent(from, event, ctx)` 返回 to，状态存业务对象 |
| 领域事件 | DomainEvent + DomainEventPublisher（`publishSync` Spring 事件 / `publishAsync` 走 Outbox 最终一致） |
| Outbox 本地消息表 | 业务 + 消息同库同事务，`OutboxRelay` 投递器异步投递 MQ，消费端幂等，实现最终一致 |

### 服务治理

| 能力 | 说明 |
|---|---|
| 统一异常与响应 | `BizException`/`SysException` + `Assert` 断言（融合 COLA）+ 统一 `Result{code,msg,data,traceId}` + `ExceptionAspect` AOP 边界捕获（只拦 `@DubboService + @ExceptionAnnotation`） |
| 用户上下文透传 | 不可变 `UserContext`（ThreadLocal）+ Dubbo `ProviderFilter`/`ConsumerFilter` 从 attachment 恢复/继续透传用户/语言/时区 |
| 数据访问 | 原生 MyBatis（SQL 显式写在 XML，便于排查）+ Druid + PageHelper 分页 + 时间字段统一 Long UTC 毫秒时间戳 |
| 缓存与锁 | Caffeine 本地缓存 + Redisson 分布式锁（单锁/多重锁/看门狗续期） |
| MQ | RocketMQ 生产模板 + 消费幂等（messageId + Redis setnx） |
| 任务调度 | XXL-Job 任务基类，入口手动创建 OpenTelemetry root span |
| 线程池 | 场景化线程池 + TTL（TransmittableThreadLocal）透传上下文 |
| 可观测性 | OpenTelemetry（`TraceContext` 读 `Span.current()`，traceId 跨服务透传） |
| 时区 | `TimeZoneUtils` 时间戳转换（存储统一 UTC 毫秒，展示层按用户时区转换） |
| 异步导出 | `AbstractExportTask` 分页查询 + 流式写文件防 OOM + 服务端上传对象存储（V1 简化版） |

## 模块结构

```
ddd-dubbo-parent
├── ddd-dubbo-api       RPC 契约：Provider 接口、Result、Req/Resp（零依赖）
├── ddd-dubbo-domain    领域模型 + 用例执行器 + 扩展点 + 状态机 + 公共治理能力
├── ddd-dubbo-infra     基础设施：Mapper、Repository 实现、Outbox 数据访问
├── ddd-dubbo-app       在线服务：Dubbo Provider、启动类、配置、MQ、Outbox 投递器
└── ddd-dubbo-job       任务进程：XXL-Job 任务基类、独立启动入口
```

依赖方向（单向，不可逆）：

```
api  ←  domain  ←  infra  ←  app / job
（零依赖）（只依赖 api）（实现 domain 接口）（组装 domain + infra）
```

## 技术基线

| 维度 | 选型 |
|---|---|
| 语言 / 运行时 | Java 8（`maven.compiler.release=8`） |
| 容器 | Spring Boot 2.7.18 |
| 服务通信 | Dubbo 2.7.23（开源版） |
| 数据访问 | 原生 MyBatis + Druid + PageHelper |
| 缓存 / 锁 | Redis / Redisson + Caffeine |
| 消息队列 | RocketMQ |
| 任务调度 | XXL-Job |
| 注册中心 | Nacos |
| 可观测性 | OpenTelemetry（traceId / spanId 跨服务透传） |
| 上下文透传 | transmittable-thread-local（TTL） |

## 构建与测试

```bash
mvn clean test      # 运行全部单测（40 用例）
mvn clean package   # 打包 5 个模块 jar
```

## 目录

```
docs/    框架详细设计文档、实施计划、专题 demo
```

## 状态

V1 已完成：五模块框架 + 完整公共能力 + 订单示例业务，40 个单测全绿。后续规划：分片上传下载（common-file）、国际化、多数据源。
