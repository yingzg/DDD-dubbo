# 分布式事务 Demo（ChainedTransactionManager vs Outbox 本地消息表）

> 本文档演示跨库/跨服务一致性的两种实现，供《框架详细设计》第 9.4 节「链式事务与跨库一致性」直接引用，代码实施 agent 可直接照此实现。
> 技术基线：Java 8 + Spring Boot 2.7.x + MyBatis + RocketMQ。
> 核心认知：**没有免费的分布式事务**。要么牺牲一致性换可用性（最终一致），要么牺牲性能换强一致（XA）。本文档两种方案都是"放弃强一致"的务实选择。

---

## 1. 为什么会有跨库事务问题

单库事务靠 `@Transactional` + 数据库 ACID 天然解决。但一旦拆库（订单库、账户库）或跨服务（订单服务、库存服务），**本地事务管不了另一个库/服务**，就会出现"订单写成功、账户扣款失败"的不一致。

| 场景 | 问题 |
|---|---|
| 跨库（订单库 + 账户库） | 两个库各提交各的，一个失败一个成功 → 不一致 |
| 跨服务（订单服务 + 库存服务） | 本地事务 + 发 MQ，事务提交前发消息会丢，提交后发消息会不一致 |

两种务实解法（都不是 XA 强一致）：
1. **ChainedTransactionManager**：链式事务，顺序提交多个本地事务（best-effort）。
2. **Outbox 本地消息表**：业务 + 消息同库同事务，异步投递（最终一致）。

---

## 2. 方案一：ChainedTransactionManager（best-effort 顺序提交）

### 2.1 原理

把一个"链式事务管理器"包装多个 `DataSourceTransactionManager`，`commit` 时**按顺序逐个提交**，`rollback` 时**逆序回滚**。本质是"把多个本地事务串起来执行"，**不是 XA 两阶段提交**。

### 2.2 完整代码

```java
package com.ddd.dubbo.infra.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperScannerConfigurer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.data.transaction.ChainedTransactionManager;

import javax.sql.DataSource;

/**
 * 多数据源 + 链式事务配置。
 * 两个库各一套 DataSource + SqlSessionFactory + TransactionManager + MapperScan。
 */
@Configuration
public class MultiDataSourceConfig {

    // ============ 库1：订单库 ============
    @Bean(name = "orderDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.order")
    public DataSource orderDataSource() {
        return new DruidDataSource();
    }

    @Bean(name = "orderSqlSessionFactory")
    public SqlSessionFactory orderSqlSessionFactory() throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(orderDataSource());
        bean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath*:mapper/order/**/*.xml"));
        return bean.getObject();
    }

    @Bean(name = "orderTransactionManager")
    public DataSourceTransactionManager orderTransactionManager() {
        return new DataSourceTransactionManager(orderDataSource());
    }

    // ============ 库2：账户库（同样一套，略）============
    @Bean(name = "accountDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.account")
    public DataSource accountDataSource() {
        return new DruidDataSource();
    }

    @Bean(name = "accountTransactionManager")
    public DataSourceTransactionManager accountTransactionManager() {
        return new DataSourceTransactionManager(accountDataSource());
    }

    // ============ 链式事务管理器 ============
    @Bean(name = "chainedTransactionManager")
    public ChainedTransactionManager chainedTransactionManager() {
        // 顺序提交：先 order 后 account
        return new ChainedTransactionManager(orderTransactionManager(), accountTransactionManager());
    }
}
```

### 2.3 使用

```java
@Service
public class OrderService {

    @Resource
    private OrderMapper orderMapper;
    @Resource
    private AccountMapper accountMapper;

    // 指定用链式事务管理器
    @Transactional(transactionManager = "chainedTransactionManager", rollbackFor = Exception.class)
    public void createOrderAndDeduct(Order order, Account account) {
        orderMapper.insert(order);          // 库1：写订单
        accountMapper.deduct(account);      // 库2：扣款
        // 两个库的操作被"串"在链式事务里
    }
}
```

### 2.4 为什么说它基本没用（反模式）

链式事务实际上**没有提供任何分布式原子性**，它和"两个独立事务 + 代码编排"效果完全一样，甚至更差（因为给人"这有原子性"的错觉）：

| 场景 | 链式事务的结果 | 普通做法（两个独立事务 + 代码编排） |
|---|---|---|
| 库1 提交失败 | 整体失败，库2 不执行 | 库1 抛异常 → 库2 不执行（**一样**） |
| 库1 成功、库2 失败 | 库1 已提交，回滚不了 → **不一致** | 库1 已提交、库2 失败 → **不一致**（**一样**） |
| 全部成功 | 正常提交 | 正常提交（**一样**） |

**所以链式事务的"逆序回滚"是无效的**：它只能回滚"还没提交的"事务管理器，但按顺序提交时，前面的早就提交了，回滚不动。

**为什么历史上会有它**：早期对分布式事务的朴素尝试，想用"一个事务管理器串多个库"模拟 XA，但做不到原子性。Spring 官方后来明确标记 `@Deprecated` 并建议不要用。

> ⚠️ 面试陷阱：如果被问"你怎么解决分布式事务"，**绝不能回答"我用 ChainedTransactionManager 解决了"**。正确回答是"这是反模式，和两个独立事务没区别，还不能提供原子性；我直接用 Outbox 本地消息表做最终一致，真要强一致上 TCC/SAGA"。

---

## 3. 方案二：Outbox 本地消息表（最终一致，推荐）

### 3.1 原理

**核心思想：把"业务操作"和"发消息"放进同一个本地事务，用"消息表"充当本地事务和 MQ 之间的桥梁。**

```
业务事务内：写业务数据 + 写 outbox 表（同库同事务，原子）
   ↓ 事务提交后
投递器：轮询 outbox 表，投递 MQ，投递成功标记 SENT
   ↓
消费端：幂等消费（messageId 去重）
```

关键点：**业务和消息表同库，本地事务保证"业务成功 ⟺ 消息落表"原子**；消息投递是异步的，失败可重试，最终一致。

### 3.2 表结构

```sql
CREATE TABLE outbox (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_type  VARCHAR(64)  NOT NULL,          -- 事件类型（如 ORDER_CREATED）
    payload     TEXT         NOT NULL,          -- 消息体（JSON）
    status      TINYINT      NOT NULL DEFAULT 0, -- 0=PENDING 1=SENT
    create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    send_time   DATETIME     NULL,               -- 投递成功时间
    retry_count INT          NOT NULL DEFAULT 0, -- 重试次数
    INDEX idx_status (status, create_time)       -- 投递器扫描索引
) ENGINE=InnoDB;
```

### 3.3 完整代码

**① 业务事务内：写业务 + 写 outbox（同库同事务）**

```java
package com.ddd.dubbo.infra.outbox;

import lombok.Data;

@Data
public class OutboxEvent {
    private Long id;
    private String eventType;
    private String payload;
    private Integer status;      // 0=PENDING 1=SENT
    private Integer retryCount;
}
```

```java
@Service
public class OrderService {

    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OutboxMapper outboxMapper;

    // 关键：业务 + outbox 在同一个本地事务里，原子
    @Transactional(rollbackFor = Exception.class)
    public void createOrder(Order order) {
        orderMapper.insert(order);                                  // ① 业务
        OutboxEvent event = new OutboxEvent();
        event.setEventType("ORDER_CREATED");
        event.setPayload(JsonUtil.toJson(new OrderCreatedMsg(order.getId())));
        outboxMapper.insert(event);                                 // ② 消息表（同事务）
    }
    // 事务提交后，业务数据和 outbox 记录同时生效；若事务回滚，两者都回滚 → 不会"业务成功但消息丢失"
}
```

**② 投递器（定时任务轮询 + 投递 MQ）**

```java
package com.ddd.dubbo.infra.outbox;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Outbox 投递器：定时扫描 PENDING 消息，投递 MQ，成功标记 SENT。
 * 投递失败不标记，下次重试（保证至少一次投递）。
 */
@Component
public class OutboxRelay {

    private static final int BATCH_SIZE = 100;

    @Resource
    private OutboxMapper outboxMapper;
    @Resource
    private MqProducerTemplate mqProducerTemplate;

    @Scheduled(fixedDelay = 5000)   // 每 5 秒扫一次
    public void relay() {
        List<OutboxEvent> pending = outboxMapper.selectPending(BATCH_SIZE);
        for (OutboxEvent e : pending) {
            try {
                mqProducerTemplate.send(e.getEventType(), e.getPayload());
                outboxMapper.markSent(e.getId());          // 投递成功标记
            } catch (Exception ex) {
                outboxMapper.increaseRetry(e.getId());     // 失败重试计数，下轮再投
            }
        }
    }
}
```

**③ 消费端幂等（至少一次投递 → 必须幂等）**

```java
@Component
@RocketMQMessageListener(topic = "order", consumerGroup = "order-consumer")
public class OrderCreatedConsumer implements RocketMQListener<OrderCreatedMsg> {

    @Resource
    private IdempotentHelper idempotentHelper;   // Redis setnx 或本地表 messageId 去重

    @Override
    public void onMessage(OrderCreatedMsg msg) {
        if (!idempotentHelper.tryConsume(msg.getMessageId())) {
            return;   // 已消费过，幂等跳过
        }
        try {
            handle(msg);
            idempotentHelper.complete(msg.getMessageId());
        } catch (Exception e) {
            // 失败重试（见 MQ 专题），超限落死信
        }
    }
}
```

### 3.4 优缺点

| 优点 | 缺点 |
|---|---|
| 业务和消息**原子**（同库同事务） | 引入 outbox 表 + 投递器，复杂度上升 |
| 投递**至少一次**，最终一致可靠 | 有投递延迟（轮询间隔） |
| 不依赖 MQ 的事务消息能力，通用 | 消息可能重复，消费端必须幂等 |

---

## 4. 两种方案对比 + 怎么选

| 维度 | ChainedTransactionManager | Outbox 本地消息表 |
|---|---|---|
| 一致性 | best-effort，**可能不一致** | **最终一致**（可靠） |
| 实现复杂度 | 低（一个事务管理器） | 中（表 + 投递器 + 幂等） |
| 适用场景 | 跨库、能补偿兜底、对账修复 | 跨服务、跨库、要求可靠最终一致 |
| 是否依赖 MQ | 否（纯 DB 事务） | 是（需要 MQ + 幂等） |
| 状态 | Spring 已 @Deprecated | 业界主流（Transactional Outbox 模式） |

**结论**：**链式事务基本无用，别用**——它和"两个独立事务 + 代码编排"效果一样，还多了一层"看起来有原子性"的误导：库1 失败普通事务就能兜住，库1 成功库2 失败它又回滚不了库1。生产直接用 **Outbox 本地消息表**（最终一致），要强一致上 TCC/SAGA，没有链式事务的位置。

---

## 5. 面试表达

> **问：跨库/跨服务怎么保证一致性？**
>
> 答：先讲清楚"没有免费的分布式事务"。我评估过 ChainedTransactionManager，结论是它基本没用——它只是顺序提交多个本地事务，不是 XA：库1 提交失败普通事务就能兜住、库1 成功库2 失败它又回滚不了库1，和两个独立事务没区别，还多了一层"有原子性"的误导，所以 Spring 废弃了它。我的方案是 Outbox 本地消息表：核心是"业务数据和消息表写在同一个本地事务里，用本地事务保证原子"，然后异步投递器轮询消息表投 MQ，消费端幂等。业务成功和消息落表是原子的，投递失败可重试，最终一致。真要强一致上 TCC/SAGA，大部分场景最终一致够了。

---

## 6. 落地清单（给代码 agent）

- [ ] 多数据源配置：两个库各一套 DataSource/SqlSessionFactory/TransactionManager/MapperScan
- [ ] 方案一：`ChainedTransactionManager` 配置 + 跨库 Service 示例
- [ ] 方案二：`outbox` 表 DDL + `OutboxEvent` + `OutboxMapper`（insert/selectPending/markSent/increaseRetry）
- [ ] `OutboxRelay` 投递器（@Scheduled 轮询 + 投递 MQ）
- [ ] 消费端幂等（IdempotentHelper）
- [ ] 单测：业务回滚时 outbox 也回滚（原子性验证）、投递失败重试
