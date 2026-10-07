# ddd-dubbo 框架实施计划（V1，已按全量 docs 对齐）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 ddd-dubbo 后端业务服务框架 V1——五模块 DDD 分层 + CmdExe/QryExe + 扩展点 + 无状态状态机 + 领域事件 + 统一异常(Assert+AOP 融合) + 用户上下文透传 + 原生 MyBatis 数据访问 + 缓存锁 + MQ(幂等/重试/死信) + Outbox 本地消息表 + XXL-Job 任务 + 线程池 + OpenTelemetry + 时区工具 + 异步导出 + 订单示例业务。

**Architecture:** Maven 五模块（api 零依赖 → domain 依赖 api+otlp-api → infra 依赖 domain → app/job 依赖 domain+infra），依赖倒置，CQRS 用例执行器 + 扩展点三级路由 + 无状态状态机 + 领域事件(Spring 同步/MQ 异步) + Outbox 最终一致。

**Tech Stack:** Java 8（`maven.compiler.release=8`，JDK 17 编译）+ Spring Boot 2.7.18 + Dubbo 2.7.23 + 原生 MyBatis + Druid + Redisson + Caffeine + RocketMQ 4.9.4 + XXL-Job 2.3.1 + transmittable-thread-local + OpenTelemetry API 1.30.0。

**测试策略:** 纯逻辑单测 TDD（JUnit 5 + Mockito），不写中间件集成测试。

**关键对齐决策（已锁定，执行时勿改）:**
1. `Result` 在 `com.ddd.dubbo.api.result`；`ErrorCode`/异常/`Assert` 在 `com.ddd.dubbo.domain.common.exception`。
2. 融合 COLA：新增 `Assert` 断言工具（ErrorCode 版）+ AOP 边界统一捕获（pointcut = `@within(ExceptionAnnotation) && @within(DubboService)`）。
3. 时间字段一律 `Long` 存 UTC 毫秒时间戳（BIGINT），新增 `TimeZoneUtils`。
4. `UserContext` 不可变（immutable）。
5. `TraceContext` 读 OpenTelemetry `Span.current()`；`AbstractTask` 手动建 OTel root span（非 UUID）。
6. 分布式事务：`ChainedTransactionManager` 不实现（反模式），实现 `Outbox` 本地消息表。
7. 不用 Lombok（手写 getter/setter），不用 MyBatis-Plus。
8. **分片上传下载（common-file，1486 行设计）→ V2 后置，不在本计划。**

---

## 文件结构总览

```
pom.xml（父，含 otel-api 版本管理）
ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/
  result/{Result,PageResult}.java
  order/{provider/OrderProvider, model/req/CreateOrderReq, model/req/QueryOrderReq, model/resp/OrderResp}.java
ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/
  exe/{Command,Query,CmdExe,QryExe,CommandBus,QueryBus}.java
  extension/{ExtensionPoint,Extension,BizScenario,ExtensionExecutor}.java
  statemachine/{StateMachine,Transition,StateMachineBuilder,StateMachineBuilderFactory}.java
  common/exception/{ErrorCode,BizException,SysException,Assert,ErrorCodeEnums,ExceptionAnnotation,ExceptionAspect}.java
  common/context/{UserContext,TraceContext}.java
  common/event/{DomainEvent,DomainEventPublisher}.java
  common/lock/LockUtil.java
  common/threadpool/ThreadPoolConfig.java
  common/time/TimeZoneUtils.java
  outbox/{OutboxEvent,OutboxRepository}.java
  order/{entity/Order, service/OrderService, repository/OrderRepository,
         command/{CreateOrderCommand,QueryOrderCommand}, exe/CreateOrderCmdExe,
         extension/{OrderDiscountExtPt,NormalOrderDiscountExt,VipOrderDiscountExt},
         statemachine/{OrderStatus,OrderEvent,OrderContext}, exception/OrderErrorCode}.java
ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/
  order/{mapper/OrderMapper, dataobject/OrderDO, converter/OrderConverter, repository/OrderRepositoryImpl}.java
  outbox/{dataobject/OutboxDO, mapper/OutboxMapper, repository/OutboxRepositoryImpl}.java
  src/main/resources/mapper/{OrderMapper.xml, OutboxMapper.xml}
ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/
  StartApplication.java
  provider/order/OrderProviderImpl.java
  config/{DataSourceConfig,DubboConfig,RedissonConfig,RedissonProperties,LocalCacheConfig}.java
  config/filter/{DubboProviderFilter,DubboConsumerFilter}.java
  mq/{MqProducerTemplate,IdempotentHelper}.java
  outbox/OutboxRelay.java
  export/{ExportRequest,ObjectStorageWriter,AbstractExportTask}.java
  src/main/resources/application.yml
ddd-dubbo-job/src/main/java/com/ddd/dubbo/job/
  JobStartApplication.java
  task/AbstractTask.java
  src/main/resources/application.yml
```

---

## Task 1: 五模块 Maven 骨架

- [ ] **Step 1: 根 POM**（与已提交版本相比，新增 otel-api 版本管理）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.7.18</version>
        <relativePath/>
    </parent>
    <groupId>com.ddd.dubbo</groupId>
    <artifactId>ddd-dubbo-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <properties>
        <java.version>1.8</java.version>
        <maven.compiler.release>8</maven.compiler.release>
        <dubbo.version>2.7.23</dubbo.version>
        <redisson.version>3.20.1</redisson.version>
        <rocketmq.version>2.2.3</rocketmq.version>
        <xxl-job.version>2.3.1</xxl-job.version>
        <ttl.version>2.14.2</ttl.version>
        <pagehelper.version>1.4.7</pagehelper.version>
        <otel.version>1.30.0</otel.version>
    </properties>
    <modules>
        <module>ddd-dubbo-api</module>
        <module>ddd-dubbo-domain</module>
        <module>ddd-dubbo-infra</module>
        <module>ddd-dubbo-app</module>
        <module>ddd-dubbo-job</module>
    </modules>
    <dependencyManagement>
        <dependencies>
            <dependency><groupId>com.ddd.dubbo</groupId><artifactId>ddd-dubbo-api</artifactId><version>${project.version}</version></dependency>
            <dependency><groupId>com.ddd.dubbo</groupId><artifactId>ddd-dubbo-domain</artifactId><version>${project.version}</version></dependency>
            <dependency><groupId>com.ddd.dubbo</groupId><artifactId>ddd-dubbo-infra</artifactId><version>${project.version}</version></dependency>
            <dependency><groupId>org.apache.dubbo</groupId><artifactId>dubbo-spring-boot-starter</artifactId><version>${dubbo.version}</version></dependency>
            <dependency><groupId>org.redisson</groupId><artifactId>redisson</artifactId><version>${redisson.version}</version></dependency>
            <dependency><groupId>org.apache.rocketmq</groupId><artifactId>rocketmq-spring-boot-starter</artifactId><version>${rocketmq.version}</version></dependency>
            <dependency><groupId>com.xuxueli</groupId><artifactId>xxl-job-core</artifactId><version>${xxl-job.version}</version></dependency>
            <dependency><groupId>com.alibaba</groupId><artifactId>transmittable-thread-local</artifactId><version>${ttl.version}</version></dependency>
            <dependency><groupId>com.github.pagehelper</groupId><artifactId>pagehelper-spring-boot-starter</artifactId><version>${pagehelper.version}</version></dependency>
            <dependency><groupId>io.opentelemetry</groupId><artifactId>opentelemetry-api</artifactId><version>${otel.version}</version></dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

- [ ] **Step 2: 各模块 POM**（api/domain/infra/app/job，与已提交版本相同，仅 domain 增 otel-api、app 增 redis）

domain 模块 POM 增加：

```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-api</artifactId>
</dependency>
```

app 模块 POM 增加：

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

- [ ] **Step 3: 编译验证** → `mvn -q compile`（首次会下载依赖，用较长 timeout）→ BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add pom.xml ddd-dubbo-*/pom.xml
git commit -m "chore: 五模块骨架（含 otel-api 依赖管理）"
```

---

## Task 2: api 统一 Result / PageResult + 订单契约

**Files:**
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/result/Result.java`
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/result/PageResult.java`
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/provider/OrderProvider.java`
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/req/CreateOrderReq.java`
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/req/QueryOrderReq.java`
- `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/resp/OrderResp.java`
- Test: `ddd-dubbo-api/src/test/java/com/ddd/dubbo/api/result/ResultTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.ddd.dubbo.api.result;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResultTest {
    @Test
    void successSetsCodeZeroAndData() {
        Result<String> r = Result.success("ok");
        assertTrue(r.isSuccess());
        assertEquals(0, r.getCode());
        assertEquals("ok", r.getData());
    }

    @Test
    void failSetsCodeAndMsg() {
        Result<Void> r = Result.fail(2001, "订单不存在");
        assertFalse(r.isSuccess());
        assertEquals(2001, r.getCode());
        assertEquals("订单不存在", r.getMsg());
    }

    @Test
    void withTraceIdAttachesTraceId() {
        Result<Void> r = Result.fail(5000, "系统异常").withTraceId("t-123");
        assertEquals("t-123", r.getTraceId());
    }
}
```

- [ ] **Step 2: 运行确认失败** → `mvn -q -pl ddd-dubbo-api test` → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.api.result;

import java.io.Serializable;

/** 统一响应模型。code=0 表示成功。纯 POJO，零框架依赖。 */
public class Result<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int SUCCESS_CODE = 0;

    private int code;
    private String msg;
    private T data;
    private String traceId;

    public static <T> Result<T> success(T data) {
        Result<T> r = new Result<T>();
        r.code = SUCCESS_CODE;
        r.msg = "success";
        r.data = data;
        return r;
    }

    public static <T> Result<T> fail(int code, String msg) {
        Result<T> r = new Result<T>();
        r.code = code;
        r.msg = msg;
        return r;
    }

    public Result<T> withTraceId(String traceId) {
        this.traceId = traceId;
        return this;
    }

    public boolean isSuccess() {
        return this.code == SUCCESS_CODE;
    }

    public int getCode() { return code; }
    public void setCode(int code) { this.code = code; }
    public String getMsg() { return msg; }
    public void setMsg(String msg) { this.msg = msg; }
    public T getData() { return data; }
    public void setData(T data) { this.data = data; }
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
}
```

```java
package com.ddd.dubbo.api.result;

import java.io.Serializable;
import java.util.List;

/** 统一分页响应 */
public class PageResult<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private long total;
    private List<T> list;

    public static <T> PageResult<T> of(long total, List<T> list) {
        PageResult<T> p = new PageResult<T>();
        p.total = total;
        p.list = list;
        return p;
    }

    public long getTotal() { return total; }
    public void setTotal(long total) { this.total = total; }
    public List<T> getList() { return list; }
    public void setList(List<T> list) { this.list = list; }
}
```

```java
package com.ddd.dubbo.api.order.provider;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.api.order.model.req.CreateOrderReq;
import com.ddd.dubbo.api.order.model.req.QueryOrderReq;
import com.ddd.dubbo.api.order.model.resp.OrderResp;

public interface OrderProvider {
    Result<OrderResp> createOrder(CreateOrderReq req);
    Result<OrderResp> queryOrder(QueryOrderReq req);
}
```

```java
package com.ddd.dubbo.api.order.model.req;

import java.io.Serializable;
import java.math.BigDecimal;

public class CreateOrderReq implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long userId;
    private String channel;
    private BigDecimal amount;

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
```

```java
package com.ddd.dubbo.api.order.model.req;

import java.io.Serializable;

public class QueryOrderReq implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long orderId;

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
}
```

```java
package com.ddd.dubbo.api.order.model.resp;

import java.io.Serializable;
import java.math.BigDecimal;

public class OrderResp implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long orderId;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount;
    private String status;
    private Long createdAt;   // UTC 毫秒时间戳

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Long getCreatedAt() { return createdAt; }
    public void setCreatedAt(Long createdAt) { this.createdAt = createdAt; }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-api
git commit -m "feat(api): Result/PageResult + 订单契约（Result 在 api.result）"
```

---

## Task 3: domain 异常体系 + Assert 断言工具

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ErrorCode.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/BizException.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/SysException.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/Assert.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ErrorCodeEnums.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/OrderErrorCode.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/exception/AssertTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.ddd.dubbo.domain.common.exception;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class AssertTest {

    @Test
    void notNullThrowsWhenNull() {
        BizException e = assertThrows(BizException.class, () -> Assert.notNull(null, OrderErrorCode.ORDER_NOT_FOUND));
        assertEquals(2001, e.getCode());
    }

    @Test
    void notNullPassesWhenNotNull() {
        Assert.notNull("x", OrderErrorCode.ORDER_NOT_FOUND);
    }

    @Test
    void isTrueThrowsWhenFalse() {
        assertThrows(BizException.class, () -> Assert.isTrue(false, OrderErrorCode.STOCK_NOT_ENOUGH));
    }

    @Test
    void notEmptyThrowsWhenEmpty() {
        assertThrows(BizException.class, () -> Assert.notEmpty(Collections.emptyList(), OrderErrorCode.ORDER_STATUS_ERROR));
    }

    @Test
    void bizExceptionCarriesCodeAndMsg() {
        BizException e = new BizException(OrderErrorCode.ORDER_NOT_FOUND);
        assertEquals(2001, e.getCode());
        assertEquals("订单不存在", e.getMessage());
    }
}
```

- [ ] **Step 2: 运行确认失败** → `mvn -q -pl ddd-dubbo-domain -am test` → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.exception;

/** 错误码接口：错误码分段 0 成功 / 1xxx 参数 / 2xxx 业务 / 5xxx 系统 */
public interface ErrorCode {
    int getCode();
    String getMsg();
}
```

```java
package com.ddd.dubbo.domain.common.exception;

/** 业务异常：可预期、对调用方可见、带业务错误码，不告警 */
public class BizException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public BizException(ErrorCode errorCode) {
        super(errorCode.getMsg());
        this.code = errorCode.getCode();
    }

    public BizException(int code, String msg) {
        super(msg);
        this.code = code;
    }

    public int getCode() { return code; }
}
```

```java
package com.ddd.dubbo.domain.common.exception;

/** 系统异常：不可预期、技术故障，msg 模糊化、要告警 */
public class SysException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int code;

    public SysException(int code, String msg, Throwable cause) {
        super(msg, cause);
        this.code = code;
    }

    public int getCode() { return code; }
}
```

```java
package com.ddd.dubbo.domain.common.exception;

import java.util.Collection;
import java.util.Map;

/** 断言工具（借鉴 COLA Assert 风格，错误码用 ErrorCode 枚举） */
public final class Assert {

    private Assert() {
    }

    public static void isTrue(boolean expression, ErrorCode errorCode) {
        if (!expression) {
            throw new BizException(errorCode);
        }
    }

    public static void isFalse(boolean expression, ErrorCode errorCode) {
        if (expression) {
            throw new BizException(errorCode);
        }
    }

    public static void notNull(Object object, ErrorCode errorCode) {
        if (object == null) {
            throw new BizException(errorCode);
        }
    }

    public static void notEmpty(Collection<?> collection, ErrorCode errorCode) {
        if (collection == null || collection.isEmpty()) {
            throw new BizException(errorCode);
        }
    }

    public static void notEmpty(Map<?, ?> map, ErrorCode errorCode) {
        if (map == null || map.isEmpty()) {
            throw new BizException(errorCode);
        }
    }
}
```

```java
package com.ddd.dubbo.domain.common.exception;

/** 通用错误码（参数/系统段） */
public enum ErrorCodeEnums implements ErrorCode {
    PARAM_IS_EMPTY(1001, "参数不能为空"),
    PARAM_IS_INVALID(1002, "参数非法"),
    SYS_ERROR(5000, "系统异常，请稍后重试");

    private final int code;
    private final String msg;

    ErrorCodeEnums(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public int getCode() { return code; }

    @Override
    public String getMsg() { return msg; }
}
```

```java
package com.ddd.dubbo.domain.common.exception;

/** 订单域业务错误码（2xxx 业务段） */
public enum OrderErrorCode implements ErrorCode {
    ORDER_NOT_FOUND(2001, "订单不存在"),
    STOCK_NOT_ENOUGH(2002, "库存不足"),
    ORDER_STATUS_ERROR(2003, "订单状态不允许此操作");

    private final int code;
    private final String msg;

    OrderErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public int getCode() { return code; }

    @Override
    public String getMsg() { return msg; }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 异常体系 + Assert 断言工具（融合 COLA）"
```

---

## Task 4: TimeZoneUtils 时区工具

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/time/TimeZoneUtils.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/time/TimeZoneUtilsTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.ddd.dubbo.domain.common.time;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TimeZoneUtilsTest {

    @Test
    void formatTimestampToShanghai() {
        long utc = 1640995200000L; // 2022-01-01 00:00:00 UTC
        assertEquals("2022-01-01 08:00:00", TimeZoneUtils.formatTimestamp(utc, "Asia/Shanghai", "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    void formatTimestampToUtc() {
        long utc = 1640995200000L;
        assertEquals("2022-01-01 00:00:00", TimeZoneUtils.formatTimestamp(utc, "UTC", "yyyy-MM-dd HH:mm:ss"));
    }

    @Test
    void emptyWhenInvalid() {
        assertEquals("", TimeZoneUtils.formatTimestamp(0L, "UTC", "yyyy-MM-dd"));
        assertEquals("", TimeZoneUtils.formatTimestamp(1640995200000L, "", "yyyy-MM-dd"));
    }

    @Test
    void getCurrentUtcTimestampPositive() {
        assertTrue(TimeZoneUtils.getCurrentUtcTimestamp() > 0);
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** 时区转换工具：统一存储 UTC 毫秒时间戳，展示层按用户时区转换 */
public final class TimeZoneUtils {

    private TimeZoneUtils() {
    }

    public static long getCurrentUtcTimestamp() {
        return Instant.now().toEpochMilli();
    }

    /** 时间戳转指定时区的格式化字符串 */
    public static String formatTimestamp(long timestamp, String timezoneId, String pattern) {
        if (timestamp <= 0 || isBlank(timezoneId) || isBlank(pattern)) {
            return "";
        }
        try {
            Instant instant = Instant.ofEpochMilli(timestamp);
            ZoneId zone = ZoneId.of(timezoneId);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
            return localDateTime.format(DateTimeFormatter.ofPattern(pattern));
        } catch (Exception e) {
            return "";
        }
    }

    /** 指定时区的字符串时间转 UTC 时间戳 */
    public static long parseToTimestamp(String timeStr, String timezoneId, String pattern) {
        if (isBlank(timeStr) || isBlank(timezoneId) || isBlank(pattern)) {
            return 0L;
        }
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern(pattern));
            return localDateTime.atZone(ZoneId.of(timezoneId)).toInstant().toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): TimeZoneUtils 时区转换工具（UTC 时间戳）"
```

---

## Task 5: CmdExe/QryExe + CommandBus/QueryBus

（代码与上一版计划 Task 5 完全一致，仅 import 路径不变。`BizException` 已在 Task 3 定义。）

- [ ] **Step 1-3: 失败测试 → 实现**（同上一版，`Command`/`Query`/`CmdExe`/`QryExe`/`CommandBus`/`QueryBus`，包 `com.ddd.dubbo.domain.exe`，`CommandBus` 用 `Map<String, CmdExe>` 构造注入 + `ResolvableType` 泛型路由）

- [ ] **Step 4: 运行确认通过** → `mvn -q -pl ddd-dubbo-domain -am test` → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): CmdExe/QryExe + CommandBus/QueryBus 泛型路由"
```

---

## Task 6: 扩展点机制

（代码与上一版计划 Task 6 完全一致，包 `com.ddd.dubbo.domain.extension`，含 `ExtensionPoint`/`Extension`/`BizScenario`/`ExtensionExecutor`）

- [ ] **Step 1-4: TDD 失败 → 实现 → 通过** → PASS

- [ ] **Step 5: 提交** → `git commit -m "feat(domain): 扩展点机制 ExtensionPoint + BizScenario 三级路由"`

---

## Task 7: 无状态状态机

（代码与上一版计划 Task 7 完全一致，包 `com.ddd.dubbo.domain.statemachine`）

- [ ] **Step 1-4: TDD** → PASS

- [ ] **Step 5: 提交** → `git commit -m "feat(domain): 无状态状态机 StateMachine + Builder"`

---

## Task 8: UserContext（不可变）+ TraceContext（OTel）

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/context/UserContext.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/context/TraceContext.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/context/UserContextTest.java`

- [ ] **Step 1: 失败测试**

```java
package com.ddd.dubbo.domain.common.context;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UserContextTest {

    @Test
    void setAndGetThenClear() {
        UserContext.set(UserContext.builder().userId(100L).locale("zh_CN").build());
        assertNotNull(UserContext.get());
        assertEquals(Long.valueOf(100L), UserContext.get().getUserId());
        assertEquals("zh_CN", UserContext.get().getLocale());
        UserContext.clear();
        assertNull(UserContext.get());
    }

    @Test
    void traceContextReturnsNullWhenNoOtel() {
        // 无 OTel Agent 时，Span.current() 为 invalid，返回 null
        assertNull(TraceContext.getTraceId());
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.context;

/** 用户上下文（不可变，ThreadLocal 持有，入口恢复 finally 清理） */
public final class UserContext {

    private static final ThreadLocal<UserContext> HOLDER = new ThreadLocal<UserContext>();

    private final Long userId;
    private final String locale;
    private final String timeZone;
    private final String tenantId;

    private UserContext(Builder builder) {
        this.userId = builder.userId;
        this.locale = builder.locale;
        this.timeZone = builder.timeZone;
        this.tenantId = builder.tenantId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UserContext get() { return HOLDER.get(); }
    public static void set(UserContext ctx) { HOLDER.set(ctx); }
    public static void clear() { HOLDER.remove(); }

    public Long getUserId() { return userId; }
    public String getLocale() { return locale; }
    public String getTimeZone() { return timeZone; }
    public String getTenantId() { return tenantId; }

    public static final class Builder {
        private Long userId;
        private String locale;
        private String timeZone;
        private String tenantId;

        public Builder userId(Long userId) { this.userId = userId; return this; }
        public Builder locale(String locale) { this.locale = locale; return this; }
        public Builder timeZone(String timeZone) { this.timeZone = timeZone; return this; }
        public Builder tenantId(String tenantId) { this.tenantId = tenantId; return this; }

        public UserContext build() {
            return new UserContext(this);
        }
    }
}
```

```java
package com.ddd.dubbo.domain.common.context;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

/** traceId 上下文：优先读 OpenTelemetry 当前 Span */
public final class TraceContext {

    private TraceContext() {
    }

    public static String getTraceId() {
        SpanContext ctx = Span.current().getSpanContext();
        return ctx != null && ctx.isValid() ? ctx.getTraceId() : null;
    }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): UserContext(不可变) + TraceContext(OTel)"
```

---

## Task 9: 领域事件

（`DomainEvent` + `DomainEventPublisher`，包 `com.ddd.dubbo.domain.common.event`。`DomainEventPublisher` 只保留 `publishSync`（Spring 事件）；`publishAsync` 依赖 MQ 模板，通过 Outbox 模式在 app 层实现，见 Task 18）

- [ ] **Step 1-3: 实现 + 编译验证** → SUCCESS

- [ ] **Step 4: 提交** → `git commit -m "feat(domain): 领域事件 DomainEvent + 同步发布器"`

---

## Task 10: LockUtil 分布式锁

（代码与上一版计划 Task 10 一致，包 `com.ddd.dubbo.domain.common.lock`，TDD 用 Mock Redisson）

- [ ] **Step 1-4: TDD** → PASS

- [ ] **Step 5: 提交** → `git commit -m "feat(domain): Redisson 分布式锁 LockUtil"`

---

## Task 11: 线程池配置

（代码与上一版计划 Task 11 一致，包 `com.ddd.dubbo.domain.common.threadpool`）

- [ ] **Step 1-2: 实现 + 编译** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(domain): 场景化线程池 + TTL"`

---

## Task 12: Outbox 领域层（OutboxEvent + OutboxRepository 接口）

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/outbox/OutboxEvent.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/outbox/OutboxRepository.java`

- [ ] **Step 1: 实现**

```java
package com.ddd.dubbo.domain.outbox;

import java.io.Serializable;

/** Outbox 事件（领域模型）：业务 + 消息表同库同事务，最终一致 */
public class OutboxEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int STATUS_PENDING = 0;
    public static final int STATUS_SENT = 1;

    private Long id;
    private String eventType;      // 事件类型，如 ORDER_CREATED
    private String payload;        // 消息体 JSON
    private Integer status;        // 0=PENDING 1=SENT
    private Integer retryCount;    // 重试次数

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public Integer getStatus() { return status; }
    public void setStatus(Integer status) { this.status = status; }
    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
}
```

```java
package com.ddd.dubbo.domain.outbox;

import java.util.List;

/** Outbox 仓储接口（domain 定义，infra 实现） */
public interface OutboxRepository {
    void save(OutboxEvent event);
    List<OutboxEvent> selectPending(int limit);
    void markSent(Long id);
    void increaseRetry(Long id);
}
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(domain): Outbox 领域模型 + 仓储接口"`

---

## Task 13: 示例业务 domain（订单）

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/entity/Order.java`（时间字段 `Long createdAt/updatedAt`）
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/repository/OrderRepository.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/service/OrderService.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/command/CreateOrderCommand.java`、`QueryOrderCommand.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/exe/CreateOrderCmdExe.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/extension/OrderDiscountExtPt.java`、`NormalOrderDiscountExt.java`、`VipOrderDiscountExt.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/statemachine/OrderStatus.java`、`OrderEvent.java`、`OrderContext.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/order/OrderServiceTest.java`

- [ ] **Step 1: 失败测试（订单领域规则 + 时间戳）**

```java
package com.ddd.dubbo.domain.order;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.service.OrderService;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderServiceTest {

    @Test
    void createOrderBuildsInitOrderWithUtcTimestamp() {
        OrderRepository repo = mock(OrderRepository.class);
        OrderService service = new OrderService(repo);
        Order order = service.createOrder(100L, new BigDecimal("50"));
        assertEquals(Long.valueOf(100L), order.getUserId());
        assertEquals(new BigDecimal("50"), order.getAmount());
        assertEquals(OrderStatus.INIT, order.getStatus());
        assertNotNull(order.getCreatedAt());
    }

    @Test
    void applyDiscountAccumulates() {
        Order order = new Order();
        order.setAmount(new BigDecimal("100"));
        order.applyDiscount(new BigDecimal("20"));
        assertEquals(new BigDecimal("20"), order.getDiscount());
    }

    @Test
    void saveDelegatesToRepository() {
        OrderRepository repo = mock(OrderRepository.class);
        OrderService service = new OrderService(repo);
        service.save(new Order());
        verify(repo).save(any(Order.class));
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现 Order（Long 时间戳）**

```java
package com.ddd.dubbo.domain.order.entity;

import java.math.BigDecimal;

/** 订单聚合根 */
public class Order {

    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount = BigDecimal.ZERO;
    private String status;
    private Long createdAt;   // UTC 毫秒时间戳
    private Long updatedAt;   // UTC 毫秒时间戳

    public void applyDiscount(BigDecimal d) {
        if (d != null) {
            this.discount = this.discount.add(d);
        }
    }

    public void pay() {
        this.status = com.ddd.dubbo.domain.order.statemachine.OrderStatus.PAID.name();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Long getCreatedAt() { return createdAt; }
    public void setCreatedAt(Long createdAt) { this.createdAt = createdAt; }
    public Long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Long updatedAt) { this.updatedAt = updatedAt; }
}
```

> 注：`Order.status` 用 `String`（存枚举名），状态枚举统一用 `com.ddd.dubbo.domain.order.statemachine.OrderStatus`，避免重复枚举。

```java
package com.ddd.dubbo.domain.order.statemachine;

public enum OrderStatus { INIT, PAID, SHIPPED, COMPLETED, CANCELLED }
```

```java
package com.ddd.dubbo.domain.order.statemachine;

public enum OrderEvent { PAY, SHIP, COMPLETE, CANCEL }
```

```java
package com.ddd.dubbo.domain.order.statemachine;

import java.math.BigDecimal;

public class OrderContext {
    private BigDecimal balance;
    private BigDecimal amount;

    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
```

- [ ] **Step 4: 实现 OrderService / OrderRepository / Command / 扩展点 / CmdExe**

```java
package com.ddd.dubbo.domain.order.repository;

import com.ddd.dubbo.domain.order.entity.Order;

public interface OrderRepository {
    Order findById(Long id);
    void save(Order order);
}
```

```java
package com.ddd.dubbo.domain.order.service;

import com.ddd.dubbo.domain.common.time.TimeZoneUtils;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;

import java.math.BigDecimal;

public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public Order createOrder(Long userId, BigDecimal amount) {
        Order order = new Order();
        order.setUserId(userId);
        order.setAmount(amount);
        order.setStatus(OrderStatus.INIT.name());
        order.setCreatedAt(TimeZoneUtils.getCurrentUtcTimestamp());
        order.setUpdatedAt(order.getCreatedAt());
        return order;
    }

    public void save(Order order) {
        orderRepository.save(order);
    }
}
```

```java
package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.domain.exe.Command;
import com.ddd.dubbo.domain.order.entity.Order;
import java.math.BigDecimal;

public class CreateOrderCommand implements Command<Order> {
    private Long userId;
    private String channel;
    private BigDecimal amount;

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
```

```java
package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.domain.exe.Query;
import com.ddd.dubbo.domain.order.entity.Order;

public class QueryOrderCommand implements Query<Order> {
    private Long orderId;

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
}
```

```java
package com.ddd.dubbo.domain.order.extension;

import com.ddd.dubbo.domain.extension.ExtensionPoint;
import com.ddd.dubbo.domain.order.entity.Order;
import java.math.BigDecimal;

public interface OrderDiscountExtPt extends ExtensionPoint {
    BigDecimal calculateDiscount(Order order);
}
```

```java
package com.ddd.dubbo.domain.order.extension;

import com.ddd.dubbo.domain.extension.Extension;
import com.ddd.dubbo.domain.order.entity.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;

@Extension(bizId = "normal", useCase = "order", scenario = "default")
@Component
public class NormalOrderDiscountExt implements OrderDiscountExtPt {
    @Override
    public BigDecimal calculateDiscount(Order order) {
        return BigDecimal.ZERO;
    }
}
```

```java
package com.ddd.dubbo.domain.order.extension;

import com.ddd.dubbo.domain.extension.Extension;
import com.ddd.dubbo.domain.order.entity.Order;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;

@Extension(bizId = "vip", useCase = "order", scenario = "default")
@Component
public class VipOrderDiscountExt implements OrderDiscountExtPt {
    @Override
    public BigDecimal calculateDiscount(Order order) {
        return order.getAmount().multiply(new BigDecimal("0.2"));
    }
}
```

```java
package com.ddd.dubbo.domain.order.exe;

import com.ddd.dubbo.domain.extension.BizScenario;
import com.ddd.dubbo.domain.extension.ExtensionExecutor;
import com.ddd.dubbo.domain.exe.CmdExe;
import com.ddd.dubbo.domain.order.command.CreateOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.extension.OrderDiscountExtPt;
import com.ddd.dubbo.domain.order.service.OrderService;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import javax.annotation.Resource;
import java.math.BigDecimal;

@Component
public class CreateOrderCmdExe implements CmdExe<CreateOrderCommand, Order> {

    @Resource
    private OrderService orderService;
    @Resource
    private ExtensionExecutor extensionExecutor;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Order execute(CreateOrderCommand cmd) {
        Order order = orderService.createOrder(cmd.getUserId(), cmd.getAmount());
        OrderDiscountExtPt ext = extensionExecutor.execute(
                OrderDiscountExtPt.class, BizScenario.of(cmd.getChannel()));
        BigDecimal discount = ext.calculateDiscount(order);
        order.applyDiscount(discount);
        orderService.save(order);
        return order;
    }
}
```

- [ ] **Step 5: 运行确认通过** → PASS

- [ ] **Step 6: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 订单示例业务（聚合/服务/扩展点/用例执行器，Long 时间戳）"
```

---

## Task 14: infra 订单数据访问（Long 时间戳）

**Files:**
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/dataobject/OrderDO.java`
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/mapper/OrderMapper.java`
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/converter/OrderConverter.java`
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/repository/OrderRepositoryImpl.java`
- `ddd-dubbo-infra/src/main/resources/mapper/OrderMapper.xml`

- [ ] **Step 1: 实现（Long 时间戳，SQL 显式写 XML）**

```java
package com.ddd.dubbo.infra.order.dataobject;

import java.math.BigDecimal;

public class OrderDO {
    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount;
    private String status;
    private Long createTime;   // UTC 毫秒时间戳
    private Long updateTime;   // UTC 毫秒时间戳

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Long getCreateTime() { return createTime; }
    public void setCreateTime(Long createTime) { this.createTime = createTime; }
    public Long getUpdateTime() { return updateTime; }
    public void setUpdateTime(Long updateTime) { this.updateTime = updateTime; }
}
```

```java
package com.ddd.dubbo.infra.order.mapper;

import com.ddd.dubbo.infra.order.dataobject.OrderDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface OrderMapper {
    OrderDO findById(@Param("id") Long id);
    int insert(OrderDO orderDO);
}
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.ddd.dubbo.infra.order.mapper.OrderMapper">

    <select id="findById" resultType="com.ddd.dubbo.infra.order.dataobject.OrderDO">
        SELECT id, user_id, amount, discount, status, create_time, update_time
        FROM t_order
        WHERE id = #{id}
    </select>

    <insert id="insert" parameterType="com.ddd.dubbo.infra.order.dataobject.OrderDO"
            useGeneratedKeys="true" keyProperty="id">
        INSERT INTO t_order (user_id, amount, discount, status, create_time, update_time)
        VALUES (#{userId}, #{amount}, #{discount}, #{status}, #{createTime}, #{updateTime})
    </insert>

</mapper>
```

```java
package com.ddd.dubbo.infra.order.converter;

import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import com.ddd.dubbo.infra.order.dataobject.OrderDO;

public final class OrderConverter {

    private OrderConverter() {
    }

    public static Order toEntity(OrderDO orderDO) {
        if (orderDO == null) {
            return null;
        }
        Order order = new Order();
        order.setId(orderDO.getId());
        order.setUserId(orderDO.getUserId());
        order.setAmount(orderDO.getAmount());
        order.setDiscount(orderDO.getDiscount());
        order.setStatus(orderDO.getStatus());
        order.setCreatedAt(orderDO.getCreateTime());
        order.setUpdatedAt(orderDO.getUpdateTime());
        return order;
    }

    public static OrderDO toDO(Order order) {
        OrderDO orderDO = new OrderDO();
        orderDO.setId(order.getId());
        orderDO.setUserId(order.getUserId());
        orderDO.setAmount(order.getAmount());
        orderDO.setDiscount(order.getDiscount());
        orderDO.setStatus(order.getStatus());
        orderDO.setCreateTime(order.getCreatedAt());
        orderDO.setUpdateTime(order.getUpdatedAt());
        return orderDO;
    }

    public static OrderResp toResp(Order order) {
        OrderResp resp = new OrderResp();
        resp.setOrderId(order.getId());
        resp.setUserId(order.getUserId());
        resp.setAmount(order.getAmount());
        resp.setDiscount(order.getDiscount());
        resp.setStatus(order.getStatus());
        resp.setCreatedAt(order.getCreatedAt());
        return resp;
    }
}
```

```java
package com.ddd.dubbo.infra.order.repository;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.infra.order.converter.OrderConverter;
import com.ddd.dubbo.infra.order.dataobject.OrderDO;
import com.ddd.dubbo.infra.order.mapper.OrderMapper;
import org.springframework.stereotype.Repository;
import javax.annotation.Resource;

@Repository
public class OrderRepositoryImpl implements OrderRepository {

    @Resource
    private OrderMapper orderMapper;

    @Override
    public Order findById(Long id) {
        OrderDO orderDO = orderMapper.findById(id);
        return OrderConverter.toEntity(orderDO);
    }

    @Override
    public void save(Order order) {
        orderMapper.insert(OrderConverter.toDO(order));
    }
}
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(infra): 订单 DO/Mapper/XML/Converter/RepositoryImpl（Long 时间戳）"`

---

## Task 15: infra Outbox 数据访问

**Files:**
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/outbox/dataobject/OutboxDO.java`
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/outbox/mapper/OutboxMapper.java`
- `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/outbox/repository/OutboxRepositoryImpl.java`
- `ddd-dubbo-infra/src/main/resources/mapper/OutboxMapper.xml`

- [ ] **Step 1: 实现**

```java
package com.ddd.dubbo.infra.outbox.dataobject;

public class OutboxDO {
    private Long id;
    private String eventType;
    private String payload;
    private Integer status;
    private Integer retryCount;
    private Long createTime;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public Integer getStatus() { return status; }
    public void setStatus(Integer status) { this.status = status; }
    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
    public Long getCreateTime() { return createTime; }
    public void setCreateTime(Long createTime) { this.createTime = createTime; }
}
```

```java
package com.ddd.dubbo.infra.outbox.mapper;

import com.ddd.dubbo.infra.outbox.dataobject.OutboxDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface OutboxMapper {
    int insert(OutboxDO outboxDO);
    List<OutboxDO> selectPending(@Param("limit") int limit);
    int markSent(@Param("id") Long id);
    int increaseRetry(@Param("id") Long id);
}
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.ddd.dubbo.infra.outbox.mapper.OutboxMapper">

    <insert id="insert" parameterType="com.ddd.dubbo.infra.outbox.dataobject.OutboxDO"
            useGeneratedKeys="true" keyProperty="id">
        INSERT INTO outbox (event_type, payload, status, retry_count, create_time)
        VALUES (#{eventType}, #{payload}, #{status}, #{retryCount}, #{createTime})
    </insert>

    <select id="selectPending" resultType="com.ddd.dubbo.infra.outbox.dataobject.OutboxDO">
        SELECT id, event_type, payload, status, retry_count, create_time
        FROM outbox
        WHERE status = 0
        ORDER BY id ASC
        LIMIT #{limit}
    </select>

    <update id="markSent">
        UPDATE outbox SET status = 1 WHERE id = #{id}
    </update>

    <update id="increaseRetry">
        UPDATE outbox SET retry_count = retry_count + 1 WHERE id = #{id}
    </update>

</mapper>
```

```java
package com.ddd.dubbo.infra.outbox.repository;

import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import com.ddd.dubbo.domain.common.time.TimeZoneUtils;
import com.ddd.dubbo.infra.outbox.dataobject.OutboxDO;
import com.ddd.dubbo.infra.outbox.mapper.OutboxMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Repository
public class OutboxRepositoryImpl implements OutboxRepository {

    @Resource
    private OutboxMapper outboxMapper;

    @Override
    public void save(OutboxEvent event) {
        OutboxDO outboxDO = new OutboxDO();
        outboxDO.setEventType(event.getEventType());
        outboxDO.setPayload(event.getPayload());
        outboxDO.setStatus(event.getStatus() == null ? OutboxEvent.STATUS_PENDING : event.getStatus());
        outboxDO.setRetryCount(event.getRetryCount() == null ? 0 : event.getRetryCount());
        outboxDO.setCreateTime(TimeZoneUtils.getCurrentUtcTimestamp());
        outboxMapper.insert(outboxDO);
    }

    @Override
    public List<OutboxEvent> selectPending(int limit) {
        List<OutboxDO> list = outboxMapper.selectPending(limit);
        List<OutboxEvent> result = new ArrayList<OutboxEvent>();
        for (OutboxDO outboxDO : list) {
            OutboxEvent e = new OutboxEvent();
            e.setId(outboxDO.getId());
            e.setEventType(outboxDO.getEventType());
            e.setPayload(outboxDO.getPayload());
            e.setStatus(outboxDO.getStatus());
            e.setRetryCount(outboxDO.getRetryCount());
            result.add(e);
        }
        return result;
    }

    @Override
    public void markSent(Long id) {
        outboxMapper.markSent(id);
    }

    @Override
    public void increaseRetry(Long id) {
        outboxMapper.increaseRetry(id);
    }
}
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(infra): Outbox 数据访问（Mapper/RepositoryImpl）"`

---

## Task 16: app 配置 + Dubbo 上下文 Filter

**Files:**
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/DataSourceConfig.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/DubboConfig.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/RedissonProperties.java`、`RedissonConfig.java`、`LocalCacheConfig.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/filter/DubboProviderFilter.java`、`DubboConsumerFilter.java`

（代码与上一版计划 Task 14 一致。`DataSourceConfig` 用原生 MyBatis SqlSessionFactory + PageHelper；`RedissonConfig`/`LocalCacheConfig`/`DubboConfig` 同上一版。`DubboProviderFilter` 用 `UserContext.builder()` 构造不可变上下文，`finally` clear。）

- [ ] **Step 1-2: 实现 + 编译** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(app): 数据源/Redisson/Caffeine/Dubbo 配置 + 上下文透传 Filter"`

---

## Task 17: app 统一异常切面（pointcut 双条件）

**Files:**
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ExceptionAnnotation.java`
- `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ExceptionAspect.java`

- [ ] **Step 1: 实现（pointcut = @within(ExceptionAnnotation) && @within(DubboService)）**

```java
package com.ddd.dubbo.domain.common.exception;

import java.lang.annotation.*;

/** 开启统一异常切面的标记注解 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ExceptionAnnotation {
}
```

```java
package com.ddd.dubbo.domain.common.exception;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.domain.common.context.TraceContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 统一异常切面：只拦「@DubboService + @ExceptionAnnotation」的类 */
@Aspect
@Component
public class ExceptionAspect {

    private static final Logger log = LoggerFactory.getLogger(ExceptionAspect.class);
    private static final int SYS_ERROR_CODE = 5000;

    @Around("@within(com.ddd.dubbo.domain.common.exception.ExceptionAnnotation) " +
            "&& @within(org.apache.dubbo.config.annotation.DubboService)")
    public Object handle(ProceedingJoinPoint pjp) {
        try {
            return pjp.proceed();
        } catch (BizException e) {
            log.info("[biz] method={} code={} msg={}", pjp.getSignature(), e.getCode(), e.getMessage());
            return Result.fail(e.getCode(), e.getMessage()).withTraceId(TraceContext.getTraceId());
        } catch (Throwable t) {
            log.error("[sys] method={}", pjp.getSignature(), t);
            return Result.fail(SYS_ERROR_CODE, "系统异常，请稍后重试").withTraceId(TraceContext.getTraceId());
        }
    }
}
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(domain): 统一异常切面（@within(DubboService) 双条件）"`

---

## Task 18: app Provider + 启动类 + OutboxRelay + MQ

**Files:**
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/StartApplication.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/provider/order/OrderProviderImpl.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/mq/MqProducerTemplate.java`、`IdempotentHelper.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/outbox/OutboxRelay.java`
- `ddd-dubbo-app/src/main/resources/application.yml`

- [ ] **Step 1: 实现**

```java
package com.ddd.dubbo.app;

import org.apache.dubbo.config.spring.context.annotation.DubboComponentScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.ddd.dubbo")
@DubboComponentScan(basePackages = "com.ddd.dubbo.app")
@EnableScheduling
public class StartApplication {
    public static void main(String[] args) {
        SpringApplication.run(StartApplication.class, args);
    }
}
```

```java
package com.ddd.dubbo.app.provider.order;

import com.ddd.dubbo.api.result.Result;
import com.ddd.dubbo.api.order.model.req.CreateOrderReq;
import com.ddd.dubbo.api.order.model.req.QueryOrderReq;
import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.api.order.provider.OrderProvider;
import com.ddd.dubbo.domain.common.exception.ExceptionAnnotation;
import com.ddd.dubbo.domain.exe.CommandBus;
import com.ddd.dubbo.domain.exe.QueryBus;
import com.ddd.dubbo.domain.order.command.CreateOrderCommand;
import com.ddd.dubbo.domain.order.command.QueryOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.infra.order.converter.OrderConverter;
import org.apache.dubbo.config.annotation.DubboService;
import javax.annotation.Resource;

@DubboService
@ExceptionAnnotation
public class OrderProviderImpl implements OrderProvider {

    @Resource
    private CommandBus commandBus;
    @Resource
    private QueryBus queryBus;

    @Override
    public Result<OrderResp> createOrder(CreateOrderReq req) {
        CreateOrderCommand cmd = new CreateOrderCommand();
        cmd.setUserId(req.getUserId());
        cmd.setChannel(req.getChannel());
        cmd.setAmount(req.getAmount());
        Order order = commandBus.dispatch(cmd);
        return Result.success(OrderConverter.toResp(order));
    }

    @Override
    public Result<OrderResp> queryOrder(QueryOrderReq req) {
        QueryOrderCommand qry = new QueryOrderCommand();
        qry.setOrderId(req.getOrderId());
        Order order = queryBus.dispatch(qry);
        return Result.success(OrderConverter.toResp(order));
    }
}
```

```java
package com.ddd.dubbo.app.mq;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;

@Component
public class MqProducerTemplate {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public void send(String topic, Object payload) {
        rocketMQTemplate.convertAndSend(topic, payload);
    }
}
```

```java
package com.ddd.dubbo.app.mq;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Component
public class IdempotentHelper {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public boolean tryConsume(String messageId) {
        Boolean ok = stringRedisTemplate.opsForValue()
                .setIfAbsent("idempotent:" + messageId, "1", 24, TimeUnit.HOURS);
        return Boolean.TRUE.equals(ok);
    }

    public void complete(String messageId) {
        stringRedisTemplate.opsForValue().set("idempotent:" + messageId + ":done", "1", 24, TimeUnit.HOURS);
    }
}
```

```java
package com.ddd.dubbo.app.outbox;

import com.ddd.dubbo.app.mq.MqProducerTemplate;
import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/** Outbox 投递器：定时扫描 PENDING 消息投递 MQ，成功标记 SENT */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    @Resource
    private OutboxRepository outboxRepository;
    @Resource
    private MqProducerTemplate mqProducerTemplate;

    @Scheduled(fixedDelay = 5000)
    public void relay() {
        List<OutboxEvent> pending = outboxRepository.selectPending(BATCH_SIZE);
        for (OutboxEvent e : pending) {
            try {
                mqProducerTemplate.send(e.getEventType(), e.getPayload());
                outboxRepository.markSent(e.getId());
            } catch (Exception ex) {
                log.error("[outbox] 投递失败 id={} type={}", e.getId(), e.getEventType(), ex);
                outboxRepository.increaseRetry(e.getId());
            }
        }
    }
}
```

```yaml
spring:
  application:
    name: ddd-dubbo
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://127.0.0.1:3306/ddd_dubbo?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC
    username: root
    password: root
    type: com.alibaba.druid.pool.DruidDataSource

dubbo:
  application:
    name: ddd-dubbo
  registry:
    address: nacos://127.0.0.1:8848
  protocol:
    name: dubbo
    port: -1

ddd:
  dubbo:
    redisson:
      host: 127.0.0.1
      port: 6379
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(app): Provider + 启动类 + OutboxRelay + MQ 模板"`

---

## Task 19: job 模块（AbstractTask 用 OTel root span）

**Files:**
- `ddd-dubbo-job/src/main/java/com/ddd/dubbo/job/JobStartApplication.java`
- `ddd-dubbo-job/src/main/java/com/ddd/dubbo/job/task/AbstractTask.java`
- `ddd-dubbo-job/src/main/resources/application.yml`

- [ ] **Step 1: 实现（OTel root span，非 UUID）**

```java
package com.ddd.dubbo.job;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.ddd.dubbo")
public class JobStartApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobStartApplication.class, args);
    }
}
```

```java
package com.ddd.dubbo.job.task;

import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.XxlJobHelper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** XXL-Job 任务基类：手动建 OTel root span，traceId 写 MDC */
public abstract class AbstractTask {

    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("ddd-dubbo-job");

    @XxlJob("taskHandler")
    public void execute() {
        String taskName = getClass().getSimpleName();
        Span span = TRACER.spanBuilder("job." + taskName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("job.name", taskName)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            MDC.put("traceId", span.getSpanContext().getTraceId());
            doExecute();
            XxlJobHelper.handleSuccess();
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            log.error("任务执行失败", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            XxlJobHelper.handleFail(e.getMessage());
        } finally {
            MDC.remove("traceId");
            span.end();
        }
    }

    protected abstract void doExecute();
}
```

```yaml
spring:
  application:
    name: ddd-dubbo-job
xxl:
  job:
    admin:
      addresses: http://127.0.0.1:8080/xxl-job-admin
    executor:
      appname: ddd-dubbo-job
      port: 9999
```

- [ ] **Step 2: 编译验证** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(job): JobStartApplication + AbstractTask（OTel root span）"`

---

## Task 20: 异步导出（AbstractExportTask + ObjectStorageWriter）

**Files:**
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/export/ExportRequest.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/export/ObjectStorageWriter.java`
- `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/export/AbstractExportTask.java`

（代码同上一版计划 Task 19，`ExportRequest` 含 taskId/bizType）

- [ ] **Step 1-2: 实现 + 编译** → SUCCESS

- [ ] **Step 3: 提交** → `git commit -m "feat(app): 异步导出任务基类（V1 简化版）"`

---

## Task 21: 全量编译 + 测试 + 收尾

- [ ] **Step 1: 全量测试** → `mvn -q clean test` → 全部单测 PASS
- [ ] **Step 2: 全量打包** → `mvn -q clean package -DskipTests` → BUILD SUCCESS
- [ ] **Step 3: 修复遗留编译/测试问题**（缺依赖、import、泛型不一致）
- [ ] **Step 4: 最终提交**

```bash
git add -A
git commit -m "feat: ddd-dubbo V1 完整实现（五模块 + 用例执行器 + 扩展点 + 状态机 + Assert + Outbox + OTel + 时区 + 示例业务）"
```

---

## 自审清单

1. **Spec 覆盖**：框架详细设计 + 统一异常Demo(Assert/包结构) + 分布式事务Demo(Outbox) + 时区指南(TimeZoneUtils/Long) + OpenTelemetry(OTel traceId) + 面试总结(immutable UserContext) 全部落地；**分片上传下载（common-file）→ V2 明确后置**（见附录）；国际化 → V2 后置（面试总结明确）。
2. **类型一致性**：`Order.status` 用 String 存枚举名，状态枚举唯一来源 `com.ddd.dubbo.domain.order.statemachine.OrderStatus`；时间字段全 `Long` UTC；`CreateOrderCommand`/`QueryOrderCommand` 泛型为 `Command<Order>`/`Query<Order>`；`Result` 在 `api.result`，`ErrorCode` 在 `domain.common.exception`。
3. **依赖方向**：domain 依赖 api + otel-api；infra 依赖 domain；app 依赖 domain+infra；job 依赖 domain+infra。Outbox 仓储接口在 domain、实现+Mapper 在 infra、投递器在 app，方向正确。

## 附录：V2 待实施（不在本计划）

- **分片上传下载能力（common-file）**：`ObjectStorageGateway` + S3 实现 + 分片会话/5 张表/状态机/3 个 Provider（详见 `docs/Dubbo通用分片上传下载能力详细设计方案.md`）。
- **国际化**：`MessageSourceHandler`/`I18nUtils`/`ErrorCodeSpec`（异常存 messageKey 懒翻译，详见面试总结第 9 节）。
- **多数据源**：单数据源预留范式，第二版复制配置类改前缀。
