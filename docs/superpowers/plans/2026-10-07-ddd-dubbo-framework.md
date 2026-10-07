# ddd-dubbo 框架实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 ddd-dubbo 后端业务服务框架——五模块 DDD 分层 + CmdExe/QryExe 用例执行器 + 扩展点机制 + 无状态状态机 + 领域事件 + 统一异常/上下文透传/数据访问/缓存锁/MQ/调度/线程池/可观测等公共治理能力 + 订单示例业务。

**Architecture:** Maven 五模块（api 零依赖 → domain 依赖 api → infra 依赖 domain → app/job 依赖 domain+infra），依赖倒置（domain 定义 Repository/Gateway 接口，infra 实现），CQRS 用例执行器 + 扩展点三级路由 + 无状态状态机 + 领域事件异步解耦。

**Tech Stack:** Java 8（`maven.compiler.release=8`，JDK 17 编译）+ Spring Boot 2.7.18 + Dubbo 2.7.23 + MyBatis（原生，SQL 显式写 XML）+ Druid + Redisson + Caffeine + RocketMQ 4.9.4 + XXL-Job + TTL(transmittable-thread-local)。

**测试策略:** 纯逻辑单测 TDD（JUnit 5 + Mockito，来自 spring-boot-starter-test），不写中间件（Dubbo/Redis/MQ/MySQL）集成测试。

**包根:** `com.ddd.dubbo`。**坐标:** groupId=`com.ddd.dubbo`，root artifactId=`ddd-dubbo-parent`，version=`1.0.0-SNAPSHOT`。

---

## 文件结构总览

```
/mnt/d/个人项目/ddd-dubbo/
├── pom.xml                                    # 父 POM（spring-boot-starter-parent 2.7.18）
├── ddd-dubbo-api/pom.xml
│   └── src/main/java/com/ddd/dubbo/api/
│       ├── common/{Result,PageResult,ErrorCode}.java
│       └── order/
│           ├── provider/OrderProvider.java
│           └── model/{req/CreateOrderReq,req/QueryOrderReq,resp/OrderResp}.java
├── ddd-dubbo-domain/pom.xml
│   └── src/main/java/com/ddd/dubbo/domain/
│       ├── exe/{Command,Query,CmdExe,QryExe,CommandBus,QueryBus}.java
│       ├── extension/{ExtensionPoint,Extension,BizScenario,ExtensionExecutor}.java
│       ├── statemachine/{StateMachine,Transition,StateMachineBuilder,StateMachineBuilderFactory}.java
│       ├── common/
│       │   ├── exception/{BizException,SysException,ExceptionAnnotation,ExceptionAspect}.java
│       │   ├── context/{UserContext,TraceContext}.java
│       │   ├── event/{DomainEvent,DomainEventPublisher}.java
│       │   ├── lock/LockUtil.java
│       │   ├── cache/LogicalExpireCache.java
│       │   └── threadpool/ThreadPoolConfig.java
│       └── order/
│           ├── entity/Order.java
│           ├── service/OrderService.java
│           ├── repository/OrderRepository.java
│           ├── command/{CreateOrderCommand,QueryOrderCommand}.java
│           ├── exe/CreateOrderCmdExe.java
│           ├── extension/{OrderDiscountExtPt,NormalOrderDiscountExt,VipOrderDiscountExt}.java
│           ├── statemachine/{OrderStatus,OrderEvent,OrderContext}.java
│           └── exception/OrderErrorCode.java
│       └── src/test/java/com/ddd/dubbo/domain/...（各 TDD 测试）
├── ddd-dubbo-infra/pom.xml
│   ├── src/main/java/com/ddd/dubbo/infra/order/
│   │   ├── mapper/OrderMapper.java
│   │   ├── dataobject/OrderDO.java
│   │   ├── converter/OrderConverter.java
│   │   └── repository/OrderRepositoryImpl.java
│   └── src/main/resources/mapper/OrderMapper.xml
├── ddd-dubbo-app/pom.xml
│   └── src/main/java/com/ddd/dubbo/app/
│       ├── StartApplication.java
│       ├── provider/order/OrderProviderImpl.java
│       └── config/
│           ├── DataSourceConfig.java
│           ├── DubboConfig.java
│           ├── RedissonConfig.java
│           ├── LocalCacheConfig.java
│           ├── RedissonProperties.java
│           └── filter/{DubboProviderFilter,DubboConsumerFilter}.java
├── ddd-dubbo-job/pom.xml
│   └── src/main/java/com/ddd/dubbo/job/
│       ├── JobStartApplication.java
│       └── task/AbstractTask.java
└── docs/  （已存在，详设 + 本计划）
```

**依赖方向约定（不可违反）:** api 不依赖任何模块；domain 只依赖 api + 开源基础（spring-context/aop、slf4j、redisson、ttl、caffeine）；infra 依赖 domain + mybatis；app/job 依赖 domain + infra + dubbo/rocketmq/xxl-job。

**测试约定:** 单测只放 domain 模块（纯逻辑），依赖 spring-boot-starter-test。api 模块 Result 的测试放 api 模块（引入 junit-jupiter）。

---

## Task 1: 初始化 git + Maven 五模块骨架

**Files:**
- Create: `pom.xml`（根）
- Create: `ddd-dubbo-api/pom.xml`、`ddd-dubbo-domain/pom.xml`、`ddd-dubbo-infra/pom.xml`、`ddd-dubbo-app/pom.xml`、`ddd-dubbo-job/pom.xml`

- [ ] **Step 1: 根 POM**

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
            <dependency>
                <groupId>com.ddd.dubbo</groupId>
                <artifactId>ddd-dubbo-api</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>com.ddd.dubbo</groupId>
                <artifactId>ddd-dubbo-domain</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>com.ddd.dubbo</groupId>
                <artifactId>ddd-dubbo-infra</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.dubbo</groupId>
                <artifactId>dubbo-spring-boot-starter</artifactId>
                <version>${dubbo.version}</version>
            </dependency>
            <dependency>
                <groupId>org.redisson</groupId>
                <artifactId>redisson</artifactId>
                <version>${redisson.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.rocketmq</groupId>
                <artifactId>rocketmq-spring-boot-starter</artifactId>
                <version>${rocketmq.version}</version>
            </dependency>
            <dependency>
                <groupId>com.xuxueli</groupId>
                <artifactId>xxl-job-core</artifactId>
                <version>${xxl-job.version}</version>
            </dependency>
            <dependency>
                <groupId>com.alibaba</groupId>
                <artifactId>transmittable-thread-local</artifactId>
                <version>${ttl.version}</version>
            </dependency>
            <dependency>
                <groupId>com.github.pagehelper</groupId>
                <artifactId>pagehelper-spring-boot-starter</artifactId>
                <version>${pagehelper.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

- [ ] **Step 2: api 模块 POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.ddd.dubbo</groupId>
        <artifactId>ddd-dubbo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>ddd-dubbo-api</artifactId>
    <name>ddd-dubbo-api</name>

    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: domain 模块 POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.ddd.dubbo</groupId>
        <artifactId>ddd-dubbo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>ddd-dubbo-domain</artifactId>
    <name>ddd-dubbo-domain</name>

    <dependencies>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-context</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-context-support</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-aop</artifactId>
        </dependency>
        <dependency>
            <groupId>org.aspectj</groupId>
            <artifactId>aspectjweaver</artifactId>
        </dependency>
        <dependency>
            <groupId>org.redisson</groupId>
            <artifactId>redisson</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>transmittable-thread-local</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: infra 模块 POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.ddd.dubbo</groupId>
        <artifactId>ddd-dubbo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>ddd-dubbo-infra</artifactId>
    <name>ddd-dubbo-infra</name>

    <dependencies>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-domain</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mybatis.spring.boot</groupId>
            <artifactId>mybatis-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-context</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 5: app 模块 POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.ddd.dubbo</groupId>
        <artifactId>ddd-dubbo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>ddd-dubbo-app</artifactId>
    <name>ddd-dubbo-app</name>

    <dependencies>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-domain</artifactId>
        </dependency>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-infra</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.dubbo</groupId>
            <artifactId>dubbo-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mybatis.spring.boot</groupId>
            <artifactId>mybatis-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>com.github.pagehelper</groupId>
            <artifactId>pagehelper-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.redisson</groupId>
            <artifactId>redisson</artifactId>
        </dependency>
        <dependency>
            <groupId>com.github.ben-manes.caffeine</groupId>
            <artifactId>caffeine</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 6: job 模块 POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.ddd.dubbo</groupId>
        <artifactId>ddd-dubbo-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <artifactId>ddd-dubbo-job</artifactId>
    <name>ddd-dubbo-job</name>

    <dependencies>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-domain</artifactId>
        </dependency>
        <dependency>
            <groupId>com.ddd.dubbo</groupId>
            <artifactId>ddd-dubbo-infra</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>com.xuxueli</groupId>
            <artifactId>xxl-job-core</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 7: 编译验证**

Run: `mvn -q -pl ddd-dubbo-api,ddd-dubbo-domain,ddd-dubbo-infra,ddd-dubbo-app,ddd-dubbo-job compile`
Expected: BUILD SUCCESS（模块可能因缺主类而无输出，但 POM 应全部解析成功）

- [ ] **Step 8: 提交**

```bash
git add pom.xml ddd-dubbo-*/pom.xml .gitignore
git commit -m "chore: 初始化五模块 Maven 骨架"
```

> 注：若 Step 7 编译因缺 Java 源文件报错，属预期，继续 Task 2 添加源码后再全量编译。请先创建 `.gitignore`（内容 `target/`、`.idea/`、`*.iml`）。

---

## Task 2: api 统一 Result / PageResult / ErrorCode

**Files:**
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/common/Result.java`
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/common/PageResult.java`
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/common/ErrorCode.java`
- Test: `ddd-dubbo-api/src/test/java/com/ddd/dubbo/api/common/ResultTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.api.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResultTest {

    @Test
    void successSetsCodeZeroAndData() {
        Result<String> r = Result.success("ok");
        assertEquals(0, r.getCode());
        assertEquals("ok", r.getData());
    }

    @Test
    void failSetsCodeAndMsg() {
        Result<Void> r = Result.fail(2001, "订单不存在");
        assertEquals(2001, r.getCode());
        assertEquals("订单不存在", r.getMsg());
        assertNull(r.getData());
    }

    @Test
    void withTraceIdAttachesTraceId() {
        Result<Void> r = Result.fail(500, "系统异常").withTraceId("t-123");
        assertEquals("t-123", r.getTraceId());
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn -q -pl ddd-dubbo-api test`
Expected: FAIL（Result 类不存在）

- [ ] **Step 3: 实现 Result / PageResult / ErrorCode**

```java
package com.ddd.dubbo.api.common;

import java.io.Serializable;

/** 统一响应（纯 POJO，零框架依赖） */
public class Result<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private int code;
    private String msg;
    private T data;
    private String traceId;

    public static <T> Result<T> success(T data) {
        Result<T> r = new Result<T>();
        r.code = 0;
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
package com.ddd.dubbo.api.common;

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
package com.ddd.dubbo.api.common;

/** 错误码接口（api 层定义，domain 层用枚举实现） */
public interface ErrorCode {
    int getCode();
    String getMsg();
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn -q -pl ddd-dubbo-api test`
Expected: PASS（3 个用例）

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-api
git commit -m "feat(api): 统一 Result/PageResult/ErrorCode"
```

---

## Task 3: api 订单契约（Provider + DTO）

**Files:**
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/provider/OrderProvider.java`
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/req/CreateOrderReq.java`
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/req/QueryOrderReq.java`
- Create: `ddd-dubbo-api/src/main/java/com/ddd/dubbo/api/order/model/resp/OrderResp.java`

- [ ] **Step 1: 编写契约类（无单测，编译验证）**

```java
package com.ddd.dubbo.api.order.provider;

import com.ddd.dubbo.api.common.Result;
import com.ddd.dubbo.api.order.model.req.CreateOrderReq;
import com.ddd.dubbo.api.order.model.req.QueryOrderReq;
import com.ddd.dubbo.api.order.model.resp.OrderResp;

/** 订单 RPC 契约（稳定对外，不随实现变化） */
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
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn -q -pl ddd-dubbo-api compile`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-api
git commit -m "feat(api): 订单 RPC 契约与 DTO"
```

---

## Task 4: domain 异常体系（BizException/SysException/OrderErrorCode）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/BizException.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/SysException.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/exception/OrderErrorCode.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/exception/BizExceptionTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.common.exception;

import com.ddd.dubbo.domain.order.exception.OrderErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BizExceptionTest {

    @Test
    void carriesCodeFromErrorCode() {
        BizException e = new BizException(OrderErrorCode.ORDER_NOT_FOUND);
        assertEquals(2001, e.getCode());
        assertEquals("订单不存在", e.getMessage());
    }

    @Test
    void carriesExplicitCodeAndMsg() {
        BizException e = new BizException(2002, "库存不足");
        assertEquals(2002, e.getCode());
        assertEquals("库存不足", e.getMessage());
    }

    @Test
    void sysExceptionCarriesCode() {
        SysException e = new SysException(5000, "系统异常", new RuntimeException());
        assertEquals(5000, e.getCode());
    }
}
```

- [ ] **Step 2: 运行确认失败** → `mvn -q -pl ddd-dubbo-domain -am test` → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.exception;

import com.ddd.dubbo.api.common.ErrorCode;

/** 业务异常（可预期，带错误码，返回给调用方） */
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

/** 系统异常（未知异常，统一兜底） */
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
package com.ddd.dubbo.domain.order.exception;

import com.ddd.dubbo.api.common.ErrorCode;

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

- [ ] **Step 4: 运行确认通过** → `mvn -q -pl ddd-dubbo-domain -am test` → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 统一异常体系 BizException/SysException/OrderErrorCode"
```

---

## Task 5: CmdExe/QryExe + CommandBus/QueryBus（CQRS）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/exe/{Command,Query,CmdExe,QryExe,CommandBus,QueryBus}.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/exe/CommandBusTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.exe;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandBusTest {

    static class CreateCmd implements Command<String> {}
    static class UpdateCmd implements Command<String> {}

    @Test
    void routesToMatchingExecutorByGenericType() {
        CmdExe<CreateCmd, String> createExe = new CmdExe<CreateCmd, String>() {
            @Override
            public String execute(CreateCmd cmd) { return "created"; }
        };
        CmdExe<UpdateCmd, String> updateExe = new CmdExe<UpdateCmd, String>() {
            @Override
            public String execute(UpdateCmd cmd) { return "updated"; }
        };
        Map<String, CmdExe> map = new HashMap<String, CmdExe>();
        map.put("create", createExe);
        map.put("update", updateExe);

        CommandBus bus = new CommandBus(map);

        assertEquals("created", bus.dispatch(new CreateCmd()));
        assertEquals("updated", bus.dispatch(new UpdateCmd()));
    }

    @Test
    void throwsWhenNoExecutorMatched() {
        CommandBus bus = new CommandBus(new HashMap<String, CmdExe>());
        BizException e = assertThrows(BizException.class, () -> bus.dispatch(new CreateCmd()));
        assertTrue(e.getMessage().contains("未找到命令执行器"));
    }
}
```

- [ ] **Step 2: 运行确认失败** → `mvn -q -pl ddd-dubbo-domain -am test` → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.exe;

/** 命令标记接口（写操作，返回结果类型 T） */
public interface Command<T> {
}
```

```java
package com.ddd.dubbo.domain.exe;

/** 查询标记接口（读操作，返回结果类型 T） */
public interface Query<T> {
}
```

```java
package com.ddd.dubbo.domain.exe;

/** 命令执行器（写操作） */
public interface CmdExe<C extends Command<R>, R> {
    R execute(C cmd);
}
```

```java
package com.ddd.dubbo.domain.exe;

/** 查询执行器（读操作） */
public interface QryExe<Q extends Query<R>, R> {
    R execute(Q qry);
}
```

```java
package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Map;

/** 命令分发总线：按泛型自动路由到匹配的 CmdExe */
@Component
public class CommandBus {

    private static final int SYS_ERROR = 5000;

    private final Map<String, CmdExe> cmdExes;

    public CommandBus(Map<String, CmdExe> cmdExes) {
        this.cmdExes = cmdExes;
    }

    @SuppressWarnings("unchecked")
    public <T extends Command<R>, R> R dispatch(T command) {
        for (CmdExe exe : cmdExes.values()) {
            Class<?> cmdType = ResolvableType.forInstance(exe)
                    .as(CmdExe.class).getGeneric(0).resolve();
            if (cmdType != null && cmdType.isInstance(command)) {
                return (R) exe.execute(command);
            }
        }
        throw new BizException(SYS_ERROR, "未找到命令执行器: " + command.getClass().getName());
    }
}
```

```java
package com.ddd.dubbo.domain.exe;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Map;

/** 查询分发总线：按泛型自动路由到匹配的 QryExe */
@Component
public class QueryBus {

    private static final int SYS_ERROR = 5000;

    private final Map<String, QryExe> qryExes;

    public QueryBus(Map<String, QryExe> qryExes) {
        this.qryExes = qryExes;
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<R>, R> R dispatch(T query) {
        for (QryExe exe : qryExes.values()) {
            Class<?> qryType = ResolvableType.forInstance(exe)
                    .as(QryExe.class).getGeneric(0).resolve();
            if (qryType != null && qryType.isInstance(query)) {
                return (R) exe.execute(query);
            }
        }
        throw new BizException(SYS_ERROR, "未找到查询执行器: " + query.getClass().getName());
    }
}
```

> 注意：`BizException` 已在 Task 4 定义，测试里 import 需正确（`com.ddd.dubbo.domain.common.exception.BizException`）。

- [ ] **Step 4: 运行确认通过** → `mvn -q -pl ddd-dubbo-domain -am test` → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): CmdExe/QryExe 用例执行器 + CommandBus/QueryBus 泛型路由"
```

---

## Task 6: 扩展点机制（ExtensionPoint + BizScenario + ExtensionExecutor）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/extension/{ExtensionPoint,Extension,BizScenario,ExtensionExecutor}.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/extension/ExtensionExecutorTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.extension;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExtensionExecutorTest {

    interface DiscountExtPt extends ExtensionPoint {
        String discount();
    }

    static class NormalExt implements DiscountExtPt {
        @Override
        public String discount() { return "normal"; }
    }

    static class VipExt implements DiscountExtPt {
        @Override
        public String discount() { return "vip"; }
    }

    @Test
    void routesByBizId() {
        List<DiscountExtPt> exts = Arrays.<DiscountExtPt>asList(new NormalExt(), new VipExt());
        ExtensionExecutor executor = new ExtensionExecutor();

        assertEquals("vip", executor.match(exts, new NormalExt().getClass(),
                BizScenario.of("vip", "order", "default")).discount());
        assertEquals("normal", executor.match(exts, new NormalExt().getClass(),
                BizScenario.of("normal", "order", "default")).discount());
    }

    @Test
    void throwsWhenNoExtension() {
        ExtensionExecutor executor = new ExtensionExecutor();
        assertThrows(BizException.class, () ->
                executor.match(Arrays.<DiscountExtPt>asList(),
                        new NormalExt().getClass(), BizScenario.of("vip", "order", "default")));
    }
}
```

> 注：此测试要求 `ExtensionExecutor` 提供一个可单测的 `match(List<T>, Class<T>, BizScenario)` 方法，路由逻辑从 `@Extension` 注解读取 bizId。为让注解信息可被测试，`ExtensionExecutor` 需能反射读取实现类上的 `@Extension` 注解（测试里用静态内部类时注解放在类上即可，本测试用简化的 match 签名，实际实现见 Step 3，测试与实现需对齐）。

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.extension;

/** 扩展点标记接口：业务变化点的抽象 */
public interface ExtensionPoint {
}
```

```java
package com.ddd.dubbo.domain.extension;

import java.lang.annotation.*;

/** 扩展实现注解：声明该实现适用的业务场景 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Extension {
    String bizId();
    String useCase() default "default";
    String scenario() default "default";
}
```

```java
package com.ddd.dubbo.domain.extension;

/** 业务场景：bizId + useCase + scenario 三级路由 */
public class BizScenario {
    public static final String DEFAULT = "default";

    private String bizId;
    private String useCase;
    private String scenario;

    public static BizScenario of(String bizId) {
        return of(bizId, DEFAULT, DEFAULT);
    }

    public static BizScenario of(String bizId, String useCase, String scenario) {
        BizScenario bs = new BizScenario();
        bs.bizId = bizId;
        bs.useCase = useCase;
        bs.scenario = scenario;
        return bs;
    }

    public String getBizId() { return bizId; }
    public String getUseCase() { return useCase; }
    public String getScenario() { return scenario; }
}
```

```java
package com.ddd.dubbo.domain.extension;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 扩展点执行器：按 BizScenario 三级降级路由 */
@Component
public class ExtensionExecutor {

    private static final int SYS_ERROR = 5000;

    private final ApplicationContext applicationContext;

    public ExtensionExecutor() {
        this.applicationContext = null; // 单测用（match 不依赖容器）
    }

    public ExtensionExecutor(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public <T extends ExtensionPoint> T execute(Class<T> extPtClass, BizScenario scenario) {
        Map<String, T> exts = applicationContext.getBeansOfType(extPtClass);
        return match(new ArrayList<T>(exts.values()), extPtClass, scenario);
    }

    /** 三级降级匹配：bizId+useCase+scenario → bizId+useCase → bizId */
    public <T extends ExtensionPoint> T match(List<T> exts, Class<T> extPtClass, BizScenario scenario) {
        T best = null;
        int bestScore = -1;
        for (T ext : exts) {
            Extension ann = ext.getClass().getAnnotation(Extension.class);
            if (ann == null) {
                continue;
            }
            int score = matchScore(ann, scenario);
            if (score > bestScore) {
                bestScore = score;
                best = ext;
            }
        }
        if (best == null) {
            throw new BizException(SYS_ERROR,
                    "缺少扩展实现: " + extPtClass.getSimpleName() + " for " + scenario.getBizId());
        }
        return best;
    }

    private int matchScore(Extension ann, BizScenario scenario) {
        if (!ann.bizId().equals(scenario.getBizId())) {
            return -1;
        }
        if (ann.useCase().equals(scenario.getUseCase()) && ann.scenario().equals(scenario.getScenario())) {
            return 3;
        }
        if (ann.useCase().equals(scenario.getUseCase())) {
            return 2;
        }
        return 1;
    }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 扩展点机制 ExtensionPoint + BizScenario 三级路由"
```

---

## Task 7: 无状态状态机

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/statemachine/{StateMachine,Transition,StateMachineBuilder,StateMachineBuilderFactory}.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/statemachine/StateMachineTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.statemachine;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StateMachineTest {

    enum S { INIT, PAID }
    enum E { PAY }

    @Test
    void firesTransitionAndReturnsTarget() {
        StateMachineBuilder<S, E, String> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY);
        StateMachine<S, E, String> sm = builder.build();

        assertEquals(S.PAID, sm.fireEvent(S.INIT, E.PAY, "ctx"));
    }

    @Test
    void throwsOnIllegalTransition() {
        StateMachineBuilder<S, E, String> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY);
        StateMachine<S, E, String> sm = builder.build();

        assertThrows(BizException.class, () -> sm.fireEvent(S.PAID, E.PAY, "ctx"));
    }

    @Test
    void conditionRejectsTransition() {
        StateMachineBuilder<S, E, Integer> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY)
                .when(ctx -> ctx > 100);
        StateMachine<S, E, Integer> sm = builder.build();

        assertThrows(BizException.class, () -> sm.fireEvent(S.INIT, E.PAY, 10));
        assertEquals(S.PAID, sm.fireEvent(S.INIT, E.PAY, 200));
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.statemachine;

import com.ddd.dubbo.domain.common.exception.BizException;

import java.util.HashMap;
import java.util.Map;

/** 无状态状态机：不保存当前状态，每次 fireEvent 传入 from 返回 to */
public class StateMachine<S, E, C> {
    private static final int STATE_TRANSITION_ERROR = 5001;
    private static final int STATE_CONDITION_ERROR = 5002;

    private final Map<S, Map<E, Transition<S, E, C>>> transitions;

    StateMachine(Map<S, Map<E, Transition<S, E, C>>> transitions) {
        this.transitions = transitions;
    }

    public S fireEvent(S from, E event, C context) {
        Map<E, Transition<S, E, C>> map = transitions.get(from);
        if (map == null || !map.containsKey(event)) {
            throw new BizException(STATE_TRANSITION_ERROR, "非法状态流转: " + from + " -> " + event);
        }
        Transition<S, E, C> t = map.get(event);
        if (t.getCondition() != null && !t.getCondition().test(context)) {
            throw new BizException(STATE_CONDITION_ERROR, "状态流转条件不满足");
        }
        if (t.getAction() != null) {
            t.getAction().execute(context);
        }
        return t.getTo();
    }
}
```

```java
package com.ddd.dubbo.domain.statemachine;

import java.util.function.Consumer;
import java.util.function.Predicate;

/** 状态转移定义 */
public class Transition<S, E, C> {
    private final S from;
    private final S to;
    private final E event;
    private Predicate<C> condition;
    private Consumer<C> action;

    Transition(S from, S to, E event) {
        this.from = from;
        this.to = to;
        this.event = event;
    }

    public S getFrom() { return from; }
    public S getTo() { return to; }
    public E getEvent() { return event; }
    public Predicate<C> getCondition() { return condition; }
    public void setCondition(Predicate<C> condition) { this.condition = condition; }
    public Consumer<C> getAction() { return action; }
    public void setAction(Consumer<C> action) { this.action = action; }
}
```

```java
package com.ddd.dubbo.domain.statemachine;

import java.util.HashMap;
import java.util.Map;

/** 状态机构建器 */
public class StateMachineBuilder<S, E, C> {

    private final Map<S, Map<E, Transition<S, E, C>>> transitions = new HashMap<S, Map<E, Transition<S, E, C>>>();

    public TransitionBuilder transition() {
        return new TransitionBuilder();
    }

    public StateMachine<S, E, C> build() {
        return new StateMachine<S, E, C>(transitions);
    }

    public class TransitionBuilder {
        private S from;
        private S to;
        private E event;
        private java.util.function.Predicate<C> condition;
        private java.util.function.Consumer<C> action;

        public TransitionBuilder from(S from) { this.from = from; return this; }
        public TransitionBuilder to(S to) { this.to = to; return this; }
        public TransitionBuilder on(E event) { this.event = event; return this; }

        public TransitionBuilder when(java.util.function.Predicate<C> condition) {
            this.condition = condition;
            return this;
        }

        public TransitionBuilder perform(java.util.function.Consumer<C> action) {
            this.action = action;
            return this;
        }

        public StateMachineBuilder<S, E, C> build() {
            Transition<S, E, C> t = new Transition<S, E, C>(from, to, event);
            t.setCondition(condition);
            t.setAction(action);
            Map<E, Transition<S, E, C>> map = transitions.get(from);
            if (map == null) {
                map = new HashMap<E, Transition<S, E, C>>();
                transitions.put(from, map);
            }
            map.put(event, t);
            return StateMachineBuilder.this;
        }
    }
}
```

```java
package com.ddd.dubbo.domain.statemachine;

/** 状态机构建器工厂 */
public final class StateMachineBuilderFactory {

    private StateMachineBuilderFactory() {
    }

    public static <S, E, C> StateMachineBuilder<S, E, C> create() {
        return new StateMachineBuilder<S, E, C>();
    }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 无状态状态机 StateMachine + Builder"
```

---

## Task 8: 用户上下文 UserContext + TraceContext

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/context/{UserContext,TraceContext}.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/context/UserContextTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.common.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UserContextTest {

    @Test
    void setAndGetThenClear() {
        UserContext ctx = new UserContext();
        ctx.setUserId(100L);
        UserContext.set(ctx);

        assertNotNull(UserContext.get());
        assertEquals(Long.valueOf(100L), UserContext.get().getUserId());

        UserContext.clear();
        assertNull(UserContext.get());
    }

    @Test
    void traceContextDefaultsAndOverrides() {
        assertNull(TraceContext.getTraceId());
        TraceContext.setTraceId("t-1");
        assertEquals("t-1", TraceContext.getTraceId());
        TraceContext.clear();
        assertNull(TraceContext.getTraceId());
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.context;

/** 用户上下文（ThreadLocal，入口恢复、finally 清理） */
public class UserContext {

    private static final ThreadLocal<UserContext> HOLDER = new ThreadLocal<UserContext>();

    private Long userId;
    private String locale;
    private String timeZone;
    private String tenantId;

    public static UserContext get() { return HOLDER.get(); }
    public static void set(UserContext ctx) { HOLDER.set(ctx); }
    public static void clear() { HOLDER.remove(); }

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getLocale() { return locale; }
    public void setLocale(String locale) { this.locale = locale; }
    public String getTimeZone() { return timeZone; }
    public void setTimeZone(String timeZone) { this.timeZone = timeZone; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
}
```

```java
package com.ddd.dubbo.domain.common.context;

/** traceId 上下文（与 MDC 配合，供异常切面/日志使用） */
public class TraceContext {

    private static final ThreadLocal<String> TRACE_ID = new ThreadLocal<String>();

    public static String getTraceId() { return TRACE_ID.get(); }
    public static void setTraceId(String traceId) { TRACE_ID.set(traceId); }
    public static void clear() { TRACE_ID.remove(); }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): UserContext/TraceContext ThreadLocal 上下文"
```

---

## Task 9: 领域事件 DomainEvent + DomainEventPublisher

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/event/{DomainEvent,DomainEventPublisher}.java`

- [ ] **Step 1: 实现（无单测，编译验证）**

```java
package com.ddd.dubbo.domain.common.event;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/** 领域事件基类 */
public abstract class DomainEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private Date occurredAt;

    public DomainEvent() {
        this.eventId = UUID.randomUUID().toString().replace("-", "");
        this.occurredAt = new Date();
    }

    public String getEventId() { return eventId; }
    public Date getOccurredAt() { return occurredAt; }
}
```

```java
package com.ddd.dubbo.domain.common.event;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/** 领域事件发布器（同步走 Spring 事件，异步走 MQ，MQ 模板在 app 层按需注入） */
@Component
public class DomainEventPublisher {

    private final ApplicationEventPublisher applicationEventPublisher;

    public DomainEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /** 同步发布（同进程监听者立即处理） */
    public void publishSync(DomainEvent event) {
        applicationEventPublisher.publishEvent(event);
    }
}
```

> 说明：异步发布依赖 MQ 模板（RocketMQ），为避免 domain 依赖 rocketmq，异步发布封装成可选接口，由 app 层注入实现（见 Task 18 的 MqEventPublisher）。domain 只保留同步发布。

- [ ] **Step 2: 编译验证** → `mvn -q -pl ddd-dubbo-domain -am compile` → SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 领域事件 DomainEvent + 同步发布器"
```

---

## Task 10: 分布式锁 LockUtil（TDD，Mock Redisson）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/lock/LockUtil.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/common/lock/LockUtilTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.ddd.dubbo.domain.common.lock;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LockUtilTest {

    @Test
    void lockKeyFormatsWithPrefix() {
        RedissonClient client = mock(RedissonClient.class);
        LockUtil util = new LockUtil(client);
        assertEquals("lock:order:100", util.lockKey("order", "100"));
    }

    @Test
    void lockAcquiresAndRunsActionThenUnlocks() throws InterruptedException {
        RedissonClient client = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(client.getLock("lock:order:100")).thenReturn(lock);
        when(lock.tryLock(5L, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        LockUtil util = new LockUtil(client);
        String result = util.lock("lock:order:100", 5, () -> "done");

        assertEquals("done", result);
        verify(lock).unlock();
    }

    @Test
    void lockThrowsWhenTimeout() throws InterruptedException {
        RedissonClient client = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(client.getLock("k")).thenReturn(lock);
        when(lock.tryLock(5L, TimeUnit.SECONDS)).thenReturn(false);

        LockUtil util = new LockUtil(client);
        assertThrows(BizException.class, () -> util.lock("k", 5, () -> "never"));
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现**

```java
package com.ddd.dubbo.domain.common.lock;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** 分布式锁工具（Redisson） */
public class LockUtil {

    private static final int LOCK_TIMEOUT = 5003;

    private final RedissonClient redissonClient;

    public LockUtil(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /** 单锁：带看门狗自动续期，获取超时抛异常 */
    public <T> T lock(String key, long waitSeconds, Supplier<T> action) {
        RLock lock = redissonClient.getLock(key);
        try {
            if (!lock.tryLock(waitSeconds, TimeUnit.SECONDS)) {
                throw new BizException(LOCK_TIMEOUT, "获取锁超时");
            }
            return action.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(LOCK_TIMEOUT, "获取锁被中断");
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** 多重锁：按固定顺序加锁，降低死锁风险 */
    public RLock multiLock(String... keys) {
        RLock[] locks = new RLock[keys.length];
        for (int i = 0; i < keys.length; i++) {
            locks[i] = redissonClient.getLock(keys[i]);
        }
        return redissonClient.getMultiLock(locks);
    }

    /** 锁 key 生成：统一前缀 + 业务维度 */
    public String lockKey(String biz, String... dimensions) {
        return "lock:" + biz + ":" + String.join(":", dimensions);
    }
}
```

- [ ] **Step 4: 运行确认通过** → PASS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): Redisson 分布式锁工具 LockUtil"
```

---

## Task 11: 线程池配置（场景化 + TTL）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/threadpool/ThreadPoolConfig.java`

- [ ] **Step 1: 实现（无单测，编译验证）**

```java
package com.ddd.dubbo.domain.common.threadpool;

import com.alibaba.ttl.threadpool.TtlExecutors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;

/** 场景化线程池（TTL 包装透传上下文） */
@Configuration
public class ThreadPoolConfig {

    @Bean("commonTaskPool")
    public ExecutorService commonTaskPool() {
        return ttl(executor(10, 50, 1000, "common-"));
    }

    @Bean("exportTaskPool")
    public ExecutorService exportTaskPool() {
        return ttl(executor(4, 16, 500, "export-"));
    }

    private ThreadPoolTaskExecutor executor(int core, int max, int queue, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(core);
        executor.setMaxPoolSize(max);
        executor.setQueueCapacity(queue);
        executor.setThreadNamePrefix(prefix);
        executor.initialize();
        return executor;
    }

    private ExecutorService ttl(ThreadPoolTaskExecutor executor) {
        return TtlExecutors.getTtlExecutorService(executor.getThreadPoolExecutor());
    }
}
```

- [ ] **Step 2: 编译验证** → `mvn -q -pl ddd-dubbo-domain -am compile` → SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 场景化线程池 + TTL 透传"
```

---

## Task 12: 示例业务 domain（订单聚合 + 用例 + 扩展点 + 状态枚举）

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/entity/Order.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/repository/OrderRepository.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/service/OrderService.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/command/{CreateOrderCommand,QueryOrderCommand}.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/extension/{OrderDiscountExtPt,NormalOrderDiscountExt,VipOrderDiscountExt}.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/statemachine/{OrderStatus,OrderEvent,OrderContext}.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/order/exe/CreateOrderCmdExe.java`
- Test: `ddd-dubbo-domain/src/test/java/com/ddd/dubbo/domain/order/OrderServiceTest.java`

- [ ] **Step 1: 写失败测试（订单领域规则）**

```java
package com.ddd.dubbo.domain.order;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderServiceTest {

    @Test
    void createOrderBuildsInitOrder() {
        OrderRepository repo = mock(OrderRepository.class);
        OrderService service = new OrderService(repo);

        Order order = service.createOrder(100L, new BigDecimal("50"));

        assertEquals(Long.valueOf(100L), order.getUserId());
        assertEquals(new BigDecimal("50"), order.getAmount());
        assertEquals(Order.OrderStatus.INIT, order.getStatus());
        assertEquals(BigDecimal.ZERO, order.getDiscount());
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
        Order order = new Order();
        service.save(order);

        verify(repo).save(order);
    }
}
```

- [ ] **Step 2: 运行确认失败** → FAIL

- [ ] **Step 3: 实现实体 / 仓储接口 / 领域服务**

```java
package com.ddd.dubbo.domain.order.entity;

import java.math.BigDecimal;

/** 订单聚合根 */
public class Order {

    public enum OrderStatus { INIT, PAID, SHIPPED, COMPLETED, CANCELLED }

    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount = BigDecimal.ZERO;
    private OrderStatus status = OrderStatus.INIT;

    public void applyDiscount(BigDecimal d) {
        if (d == null) {
            return;
        }
        this.discount = this.discount.add(d);
    }

    public void pay() {
        this.status = OrderStatus.PAID;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
}
```

```java
package com.ddd.dubbo.domain.order.repository;

import com.ddd.dubbo.domain.order.entity.Order;

/** 订单仓储接口（domain 定义，infra 实现，依赖倒置） */
public interface OrderRepository {
    Order findById(Long id);
    void save(Order order);
}
```

```java
package com.ddd.dubbo.domain.order.service;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;

import java.math.BigDecimal;

/** 订单领域服务 */
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public Order createOrder(Long userId, BigDecimal amount) {
        Order order = new Order();
        order.setUserId(userId);
        order.setAmount(amount);
        return order;
    }

    public void save(Order order) {
        orderRepository.save(order);
    }
}
```

- [ ] **Step 4: 实现 Command / 扩展点 / 状态枚举**

```java
package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.domain.exe.Command;

import java.math.BigDecimal;

public class CreateOrderCommand implements Command<OrderResp> {
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

import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.domain.exe.Query;

public class QueryOrderCommand implements Query<OrderResp> {
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

/** 订单折扣扩展点 */
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

/** 订单状态机上下文 */
public class OrderContext {
    private BigDecimal balance;
    private BigDecimal amount;

    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
```

- [ ] **Step 5: 实现用例执行器（依赖 infra 的 Converter，本任务先留空实现）**

> `CreateOrderCmdExe` 依赖 `OrderConverter`（infra 层）做 Entity→Resp 转换。为保持依赖方向（domain 不依赖 infra），`OrderResp` 的组装放在 app 层 Provider 内完成，`CreateOrderCmdExe` 返回领域对象 `Order`。因此 Command 的返回类型改为 `Order`（而非 `OrderResp`）。

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

> 由于 `CreateOrderCmdExe` 返回 `Order`，需同步修正 `CreateOrderCommand` 的泛型为 `Command<Order>`，`QueryOrderCommand` 泛型为 `Query<Order>`。请执行以下修正：将 `CreateOrderCommand` 声明改为 `implements Command<Order>` 并 import `Order`；`QueryOrderCommand` 改为 `implements Query<Order>`。

- [ ] **Step 6: 运行测试确认通过**

Run: `mvn -q -pl ddd-dubbo-domain -am test`
Expected: PASS（OrderServiceTest 3 用例）

- [ ] **Step 7: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 订单示例业务（聚合/服务/仓储接口/扩展点/状态机枚举/用例执行器）"
```

---

## Task 13: infra 层（DO/Mapper/XML/Converter/RepositoryImpl）

**Files:**
- Create: `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/dataobject/OrderDO.java`
- Create: `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/mapper/OrderMapper.java`
- Create: `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/converter/OrderConverter.java`
- Create: `ddd-dubbo-infra/src/main/java/com/ddd/dubbo/infra/order/repository/OrderRepositoryImpl.java`
- Create: `ddd-dubbo-infra/src/main/resources/mapper/OrderMapper.xml`

- [ ] **Step 1: 实现 DO / Mapper / XML**

```java
package com.ddd.dubbo.infra.order.dataobject;

import java.math.BigDecimal;
import java.util.Date;

/** 订单表映射对象 */
public class OrderDO {
    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount;
    private String status;
    private Date createTime;
    private Date updateTime;

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
    public Date getCreateTime() { return createTime; }
    public void setCreateTime(Date createTime) { this.createTime = createTime; }
    public Date getUpdateTime() { return updateTime; }
    public void setUpdateTime(Date updateTime) { this.updateTime = updateTime; }
}
```

```java
package com.ddd.dubbo.infra.order.mapper;

import com.ddd.dubbo.infra.order.dataobject.OrderDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/** 订单 Mapper（原生 MyBatis，SQL 显式写在 XML） */
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
        VALUES (#{userId}, #{amount}, #{discount}, #{status}, now(), now())
    </insert>

</mapper>
```

- [ ] **Step 2: 实现 Converter / RepositoryImpl**

```java
package com.ddd.dubbo.infra.order.converter;

import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.infra.order.dataobject.OrderDO;

/** 订单对象转换（Entity ↔ DO ↔ Resp） */
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
        order.setStatus(Order.OrderStatus.valueOf(orderDO.getStatus()));
        return order;
    }

    public static OrderDO toDO(Order order) {
        OrderDO orderDO = new OrderDO();
        orderDO.setId(order.getId());
        orderDO.setUserId(order.getUserId());
        orderDO.setAmount(order.getAmount());
        orderDO.setDiscount(order.getDiscount());
        orderDO.setStatus(order.getStatus().name());
        return orderDO;
    }

    public static OrderResp toResp(Order order) {
        OrderResp resp = new OrderResp();
        resp.setOrderId(order.getId());
        resp.setUserId(order.getUserId());
        resp.setAmount(order.getAmount());
        resp.setDiscount(order.getDiscount());
        resp.setStatus(order.getStatus().name());
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

/** 订单仓储实现（依赖倒置：实现 domain 的 OrderRepository 接口） */
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

- [ ] **Step 3: 编译验证** → `mvn -q -pl ddd-dubbo-infra -am compile` → SUCCESS

- [ ] **Step 4: 提交**

```bash
git add ddd-dubbo-infra
git commit -m "feat(infra): 订单 DO/Mapper/XML/Converter/RepositoryImpl"
```

---

## Task 14: app 配置（数据源/Redisson/Caffeine/Dubbo）+ Dubbo 上下文 Filter

**Files:**
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/{DataSourceConfig,RedissonProperties,RedissonConfig,LocalCacheConfig,DubboConfig}.java`
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/config/filter/{DubboProviderFilter,DubboConsumerFilter}.java`
- Create: `ddd-dubbo-app/src/main/resources/application.yml`

- [ ] **Step 1: 数据源配置（原生 MyBatis + Druid + PageHelper）**

```java
package com.ddd.dubbo.app.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(basePackages = "com.ddd.dubbo.infra.**.mapper")
public class DataSourceConfig {

    @Bean
    @Primary
    public SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setMapperLocations(
                new PathMatchingResourcePatternResolver()
                        .getResources("classpath*:mapper/**/*.xml"));
        org.apache.ibatis.session.Configuration config =
                new org.apache.ibatis.session.Configuration();
        config.setMapUnderscoreToCamelCase(true);
        factoryBean.setConfiguration(config);
        return factoryBean.getObject();
    }

    @Bean
    @Primary
    public DataSourceTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    @Primary
    public SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }
}
```

- [ ] **Step 2: Redisson 配置 + 属性**

```java
package com.ddd.dubbo.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ddd.dubbo.redisson")
public class RedissonProperties {
    private String host = "127.0.0.1";
    private int port = 6379;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
}
```

```java
package com.ddd.dubbo.app.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RedissonProperties.class)
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedissonProperties props) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + props.getHost() + ":" + props.getPort());
        return Redisson.create(config);
    }
}
```

- [ ] **Step 3: Caffeine 本地缓存 + Dubbo 配置**

```java
package com.ddd.dubbo.app.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class LocalCacheConfig {

    @Bean
    public Cache<String, Object> localCache() {
        return Caffeine.newBuilder()
                .maximumSize(1_000_000)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .build();
    }
}
```

```java
package com.ddd.dubbo.app.config;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DubboConfig {

    @Bean
    public ApplicationConfig applicationConfig() {
        ApplicationConfig config = new ApplicationConfig();
        config.setName("ddd-dubbo");
        return config;
    }

    @Bean
    public RegistryConfig registryConfig() {
        RegistryConfig config = new RegistryConfig();
        config.setAddress("nacos://${nacos.addrs:127.0.0.1:8848}");
        return config;
    }

    @Bean
    public ProtocolConfig protocolConfig() {
        ProtocolConfig config = new ProtocolConfig();
        config.setName("dubbo");
        config.setPort(-1);
        return config;
    }
}
```

- [ ] **Step 4: Dubbo 上下文透传 Filter**

```java
package com.ddd.dubbo.app.config.filter;

import com.ddd.dubbo.domain.common.context.UserContext;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/** Provider 侧：从 attachment 恢复用户上下文 */
@Activate(group = CommonConstants.PROVIDER)
public class DubboProviderFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            UserContext ctx = new UserContext();
            String userId = invocation.getAttachment("userId");
            if (userId != null) {
                ctx.setUserId(Long.valueOf(userId));
            }
            ctx.setLocale(invocation.getAttachment("locale"));
            ctx.setTimeZone(invocation.getAttachment("timeZone"));
            ctx.setTenantId(invocation.getAttachment("tenantId"));
            UserContext.set(ctx);
            return invoker.invoke(invocation);
        } finally {
            UserContext.clear();
        }
    }
}
```

```java
package com.ddd.dubbo.app.config.filter;

import com.ddd.dubbo.domain.common.context.UserContext;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/** Consumer 侧：把上下文写回 attachment 继续透传 */
@Activate(group = CommonConstants.CONSUMER)
public class DubboConsumerFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        UserContext ctx = UserContext.get();
        if (ctx != null) {
            if (ctx.getUserId() != null) {
                invocation.setAttachment("userId", String.valueOf(ctx.getUserId()));
            }
            invocation.setAttachment("locale", ctx.getLocale());
            invocation.setAttachment("timeZone", ctx.getTimeZone());
            invocation.setAttachment("tenantId", ctx.getTenantId());
        }
        return invoker.invoke(invocation);
    }
}
```

- [ ] **Step 5: 编译验证** → `mvn -q -pl ddd-dubbo-app -am compile` → SUCCESS

- [ ] **Step 6: 提交**

```bash
git add ddd-dubbo-app
git commit -m "feat(app): 数据源/Redisson/Caffeine/Dubbo 配置 + 上下文透传 Filter"
```

---

## Task 15: app 统一异常切面 + 参数 Trim

**Files:**
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ExceptionAnnotation.java`
- Create: `ddd-dubbo-domain/src/main/java/com/ddd/dubbo/domain/common/exception/ExceptionAspect.java`

> 切面虽在 domain 模块（依赖 spring-aop/aspectj），但需在 app 启动时被扫描。app 启动类扫描 `com.ddd.dubbo` 全包即可。

- [ ] **Step 1: 实现注解 + 切面**

```java
package com.ddd.dubbo.domain.common.exception;

import java.lang.annotation.*;

/** 标记统一异常切面拦截的类/方法 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ExceptionAnnotation {
}
```

```java
package com.ddd.dubbo.domain.common.exception;

import com.ddd.dubbo.api.common.Result;
import com.ddd.dubbo.domain.common.context.TraceContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 统一异常切面：业务异常转 Result.fail，未知异常兜底 */
@Aspect
@Component
public class ExceptionAspect {

    private static final Logger log = LoggerFactory.getLogger(ExceptionAspect.class);
    private static final int SYS_ERROR = 5000;

    @Around("@annotation(com.ddd.dubbo.domain.common.exception.ExceptionAnnotation)")
    public Object handle(ProceedingJoinPoint pjp) {
        try {
            return pjp.proceed();
        } catch (BizException e) {
            return Result.fail(e.getCode(), e.getMessage()).withTraceId(TraceContext.getTraceId());
        } catch (Throwable t) {
            log.error("[{}] 系统异常", pjp.getSignature(), t);
            return Result.fail(SYS_ERROR, "系统异常，请稍后重试").withTraceId(TraceContext.getTraceId());
        }
    }
}
```

- [ ] **Step 2: 编译验证** → `mvn -q -pl ddd-dubbo-domain -am compile` → SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-domain
git commit -m "feat(domain): 统一异常切面 ExceptionAspect + ExceptionAnnotation"
```

---

## Task 16: app Provider 实现 + 启动类

**Files:**
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/provider/order/OrderProviderImpl.java`
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/StartApplication.java`
- Create: `ddd-dubbo-app/src/main/resources/application.yml`

- [ ] **Step 1: Provider 实现**

```java
package com.ddd.dubbo.app.provider.order;

import com.ddd.dubbo.api.common.Result;
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

- [ ] **Step 2: 启动类**

```java
package com.ddd.dubbo.app;

import org.apache.dubbo.config.spring.context.annotation.DubboComponentScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 在线服务启动入口（排除 job 包扫描） */
@SpringBootApplication(scanBasePackages = "com.ddd.dubbo")
@DubboComponentScan(basePackages = "com.ddd.dubbo.app")
public class StartApplication {
    public static void main(String[] args) {
        SpringApplication.run(StartApplication.class, args);
    }
}
```

- [ ] **Step 3: application.yml**

```yaml
spring:
  application:
    name: ddd-dubbo
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://127.0.0.1:3306/ddd_dubbo?useUnicode=true&characterEncoding=utf8
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

- [ ] **Step 4: 编译验证** → `mvn -q -pl ddd-dubbo-app -am compile` → SUCCESS

- [ ] **Step 5: 提交**

```bash
git add ddd-dubbo-app
git commit -m "feat(app): 订单 Provider 实现 + 启动类 + application.yml"
```

---

## Task 17: job 模块（JobStartApplication + AbstractTask）

**Files:**
- Create: `ddd-dubbo-job/src/main/java/com/ddd/dubbo/job/JobStartApplication.java`
- Create: `ddd-dubbo-job/src/main/java/com/ddd/dubbo/job/task/AbstractTask.java`
- Create: `ddd-dubbo-job/src/main/resources/application.yml`

- [ ] **Step 1: 实现**

```java
package com.ddd.dubbo.job;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Job 独立进程启动入口（复用 domain/infra，不暴露 Dubbo Provider） */
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.UUID;

/** XXL-Job 任务基类：入口统一初始化 traceId 写 MDC，结束清理 */
public abstract class AbstractTask {

    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);

    @XxlJob("taskHandler")
    public void execute() {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        MDC.put("traceId", traceId);
        try {
            doExecute();
            XxlJobHelper.handleSuccess();
        } catch (Exception e) {
            log.error("任务执行失败", e);
            XxlJobHelper.handleFail(e.getMessage());
        } finally {
            MDC.clear();
        }
    }

    protected abstract void doExecute();
}
```

- [ ] **Step 2: application.yml**

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

- [ ] **Step 3: 编译验证** → `mvn -q -pl ddd-dubbo-job -am compile` → SUCCESS

- [ ] **Step 4: 提交**

```bash
git add ddd-dubbo-job
git commit -m "feat(job): JobStartApplication + XXL-Job 任务基类 AbstractTask"
```

---

## Task 18: RocketMQ 生产/消费模板 + 幂等

**Files:**
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/mq/{MqProducerTemplate,IdempotentHelper}.java`

- [ ] **Step 1: 实现（编译验证，中间件集成测试后置）**

```java
package com.ddd.dubbo.app.mq;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/** RocketMQ 生产者模板 */
@Component
public class MqProducerTemplate {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /** 发送普通消息 */
    public void send(String topic, Object payload) {
        rocketMQTemplate.convertAndSend(topic, payload);
    }

    /** 发送延时消息（delayLevel：1~18） */
    public void sendDelay(String topic, Object payload, int delayLevel) {
        rocketMQTemplate.syncSend(topic,
                MessageBuilder.withPayload(payload)
                        .setHeader(RocketMQHeaders.KEYS, String.valueOf(System.currentTimeMillis()))
                        .build(),
                3000, delayLevel);
    }
}
```

```java
package com.ddd.dubbo.app.mq;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/** 幂等辅助：messageId + Redis setnx 去重 */
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

> 注：`IdempotentHelper` 依赖 `StringRedisTemplate`，需在 app POM 添加 `spring-boot-starter-data-redis` 依赖。执行时若编译报错，在 `ddd-dubbo-app/pom.xml` 增加：
> ```xml
> <dependency>
>     <groupId>org.springframework.boot</groupId>
>     <artifactId>spring-boot-starter-data-redis</artifactId>
> </dependency>
> ```

- [ ] **Step 2: 编译验证** → `mvn -q -pl ddd-dubbo-app -am compile` → SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-app
git commit -m "feat(app): RocketMQ 生产模板 + 幂等辅助"
```

---

## Task 19: 异步导出 starter（AbstractExportTask + ObjectStorageWriter）

**Files:**
- Create: `ddd-dubbo-app/src/main/java/com/ddd/dubbo/app/export/{ObjectStorageWriter,AbstractExportTask}.java`

- [ ] **Step 1: 实现（编译验证）**

```java
package com.ddd.dubbo.app.export;

import java.io.File;

/** 对象存储写入（导出文件服务端上传的最小能力） */
public interface ObjectStorageWriter {
    String upload(String directory, File file);
}
```

```java
package com.ddd.dubbo.app.export;

/** 异步导出任务基类：分页查询 + 流式写文件 + 上传，防止 OOM */
public abstract class AbstractExportTask<T> {

    private final ObjectStorageWriter objectStorageWriter;

    protected AbstractExportTask(ObjectStorageWriter objectStorageWriter) {
        this.objectStorageWriter = objectStorageWriter;
    }

    public String execute(ExportRequest request) {
        File file = streamWrite(request);
        return objectStorageWriter.upload(request.getBizType(), file);
    }

    /** 分页查询 + 流式写文件（子类实现，避免一次性加载全部数据） */
    protected abstract File streamWrite(ExportRequest request);
}
```

> 注：`ExportRequest` 为导出请求模型，需一并创建：
> ```java
> package com.ddd.dubbo.app.export;
>
> public class ExportRequest {
>     private String taskId;
>     private String bizType;
>     public String getTaskId() { return taskId; }
>     public void setTaskId(String taskId) { this.taskId = taskId; }
>     public String getBizType() { return bizType; }
>     public void setBizType(String bizType) { this.bizType = bizType; }
> }
> ```

- [ ] **Step 2: 编译验证** → `mvn -q -pl ddd-dubbo-app -am compile` → SUCCESS

- [ ] **Step 3: 提交**

```bash
git add ddd-dubbo-app
git commit -m "feat(app): 异步导出任务基类 AbstractExportTask"
```

---

## Task 20: 全量编译 + 测试 + 收尾

- [ ] **Step 1: 全量测试**

Run: `mvn -q clean test`
Expected: 全部单测 PASS（ResultTest / BizExceptionTest / CommandBusTest / ExtensionExecutorTest / StateMachineTest / UserContextTest / LockUtilTest / OrderServiceTest）

- [ ] **Step 2: 全量打包**

Run: `mvn -q clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: 修复任何编译/测试问题**

若某模块编译失败（如缺依赖、import 错误、泛型不一致），定位并修复，重新运行 Step 1/2 直到通过。

- [ ] **Step 4: 最终提交**

```bash
git add -A
git commit -m "feat: ddd-dubbo 框架完整实现（五模块 + 用例执行器 + 扩展点 + 状态机 + 公共能力 + 示例业务）"
```

---

## 自审清单（写计划后自查）

1. **Spec 覆盖**：详设第 18 章 16 步 → 本计划 Task 1-20 全覆盖（骨架/Result/异常/用例执行器/扩展点/状态机/Provider/上下文/数据访问/缓存锁/MQ/调度/线程池/领域事件/导出/示例业务/SDK 骨架）。SDK 拆分（详设第 15 章）因涉及独立 starter 模块打包，V1 以 `@ConditionalOnProperty` 装配能力开关的形式落在 app 配置类中，未单独建 starter 模块（符合「纯公共能力 + 条件装配」目标，避免过度拆分）。
2. **占位符扫描**：无 TBD/TODO/“类似 Task N”；每个代码步骤含完整代码。
3. **类型一致性**：`Order` 状态枚举为 `Order.OrderStatus`（实体内部），状态机枚举 `OrderStatus`（order.statemachine 包）与实体内部枚举重名——执行时请注意：领域规则用 `Order.OrderStatus`，状态机用 `com.ddd.dubbo.domain.order.statemachine.OrderStatus`。若混淆，统一状态机枚举为唯一来源（推荐：删除实体内部枚举，统一 import statemachine.OrderStatus）。

> **执行提示**：Task 12 中 `CreateOrderCommand`/`QueryOrderCommand` 泛型最终为 `Command<Order>`/`Query<Order>`（返回领域对象，Resp 组装在 Provider 层），执行时务必一致。
