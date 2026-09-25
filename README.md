# Reorg-Safe Block Indexer（防重组区块索引服务）

一个基于 Spring Boot 3 / Java 21 的本地区块索引服务。区块数据**只来自进程内模拟源**
（`LocalBlockSource`），不依赖任何外部网络、第三方节点或真实凭据。服务保证在
**链重组、重复同步、并发触发与数据源故障**下，索引状态始终一致、水位单调、失败可解释。

---

## 1. 一致性模型

### 1.1 不可变快照 + 原子引用（快照隔离读）

`IndexStore` 内部只持有一个 `AtomicReference<ChainSnapshot>`：

- `ChainSnapshot` 是**不可变**对象（区块列表 `List.copyOf` 固化），表示某一次完整提交后的规范化链；
- 所有查询（`snapshot()` / `blockAtHeight` / `blockByHash` / HTTP GET）都读当前引用，**无锁**；
- 同步进行中，引用仍指向上一个完整快照，因此对外查询**永远不可能看到回滚一半或写入一半的状态**，
  只可能读到“旧的完整状态”或“新的完整状态”。

### 1.2 先拉取，后提交（无部分写入）

一次同步分两阶段：

1. **取数阶段**：从某个数据源依次取回需要的全部区块，在内存中校验高度连续、父子相接；
2. **提交阶段**：`IndexStore.commit` 在私有草稿上完成「回滚失效段 + 拼接新段」，
   再用 **CAS** 整体替换快照引用。

取数阶段抛出的任何错误（超时 / 传输 / 业务）都发生在写入之前，已取回的区块随草稿一起丢弃，
**本地状态零改动**。

### 1.3 链重组（reorg）规则

同步时从本地水位沿高度向下与数据源逐块比对哈希：

- 找到第一个哈希相同的高度 `h`，则分叉点 `forkHeight = h + 1`；创世即分叉则为 0；
- 回滚 `[forkHeight, oldTip]` 的全部区块，重放数据源上 `[forkHeight, newTip]` 的新区块；
- 回滚与重放在**同一次原子提交**内完成，提交结果中包含 `rollbackFromHeight`、
  `rolledBackHashes`、`appliedHashes`，并在日志中打印回滚区间。

### 1.4 水位单调（最长链规则）

- 索引水位（已提交链头高度）**只增不减**；
- 只接受**严格长于**本地链的源数据。源高度与本地持平时：
  - tip 哈希相同 → `ALREADY_SYNCED`（幂等跳过）；
  - tip 哈希不同（等长分叉）→ 拒绝该源并归为 `BUSINESS`，绝不重放等长链；
- 源高度落后本地 → 视为能力不足，拒绝并切换下一个源；
- 存储层对「高度不连续 / 未接在回滚点之后 / 提交导致水位倒退」直接抛异常兜底。

### 1.5 并发与重复投递

- `sync()` 与 `submitBlock()` 共用一把 `ReentrantLock`；
- `sync()` 使用 `tryLock`：并发触发时只有一个真正执行，其余立即返回 `BUSY_SKIPPED`（不排队、不重复写）；
- 重复同步返回 `ALREADY_SYNCED`；重复投递同一区块返回 `ALREADY_SYNCED`，不产生重复行；
- 乱序 / 缺口 / 孤块 / 同高度冲突哈希的投递返回 `REJECTED`，**不产生任何写入**
  （分叉重放只能通过 `sync` 完成，避免单块投递绕过重组检测）；
- 锁只串行化写，读路径完全无锁，因此同步进行中查询始终可用。

---

## 2. 数据源兼容与切换规则

### 2.1 数据源接口

```java
interface BlockSource {
    String name();                                   // 用于日志与失败归因
    long headHeight() throws BlockSourceException;   // 链头高度
    Optional<Block> blockAtHeight(long h) ...;       // 按高度取块
    Optional<Block> blockByHash(String hash) ...;    // 按哈希取块
}
```

所有实现都在本进程内。`LocalBlockSource` 支持测试/演示控制：

| 方法 | 作用 |
| --- | --- |
| `replaceChain(list)` | 整体替换链，模拟源侧发生重组 |
| `setFailure(ex)` | 注入 TIMEOUT / TRANSPORT / BUSINESS 故障，`null` 清除 |
| `setLatencyMillis(ms)` | 注入固定延迟，配合验证超时语义 |
| `setGate(latch)` / `setOnAwaitGate(cb)` | 让取数请求确定性地阻塞在中途，验证“同步中可读” |

### 2.2 失败分类（可区分、可归因）

所有源错误继承 `BlockSourceException`，通过 `kind()` 机器可区分：

| 异常 | kind | 含义 |
| --- | --- | --- |
| `SourceTimeoutException` | `TIMEOUT` | 规定时间内未响应 |
| `SourceTransportException` | `TRANSPORT` | 连接断开 / IO / 中断 |
| `SourceBusinessException` | `BUSINESS` | 源可达但数据不被接受：区块缺失、高度字段不一致、源落后、等长分叉、内容损坏 |

### 2.3 选择与切换顺序

1. **链头探测**：先向所有源调用 `headHeight()`。探测失败按上表分类记录，源被移出候选；
2. **最高高度为准**：比较成功源的高度，落后于最高高度的源记为 `BUSINESS`（能力不足）——
   不同源高度不一致时，低高度源的数据**不会**被采用，避免污染；
3. **按优先级尝试**：在最高高度候选源中按注入顺序逐个尝试「完整取数 + 提交」；
   某个源取数中途失败（例如高度 3 损坏），本次取数整体作废，切换下一个源**从头**尝试，
   绝不拼接来自不同源的区块；
4. **全部失败**：返回 `ALL_SOURCES_UNAVAILABLE`，结果中 `attempts` 给出每个源的
   `sourceName / success / failureKind / detail`，本地水位与链保持不变。

---

## 3. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET  /api/index/status` | 当前水位、链长度与规范化链视图 |
| `GET  /api/index/blocks/{height}` | 按高度查区块 |
| `GET  /api/index/blocks/by-hash/{hash}` | 按哈希查区块 |
| `POST /api/index/sync` | 触发一次同步，返回可解释结果 |
| `POST /api/index/blocks` | 直接投递一个区块（JSON：height/hash/parentHash/payload） |

`SyncResult` 字段：`status`、`watermarkBefore`、`watermarkAfter`、`rollbackFromHeight`、
`rolledBackHashes`、`appliedHashes`、`attempts[]`、`message`。

`status` 取值：`ADVANCED` / `REORGANIZED` / `ALREADY_SYNCED` / `BUSY_SKIPPED` /
`REJECTED` / `ALL_SOURCES_UNAVAILABLE`。

---

## 4. 本地验证方法

要求 JDK 21+、Maven 3.9+（已验证可离线构建）。

```bash
# 编译
mvn -o compile

# 运行全部单测（无需网络；Spring 上下文测试 + 三组核心场景）
mvn -o test

# 启动服务（随机端口；启动后可调用接口）
mvn -o spring-boot:run
# 另一个终端：
curl -s -X POST http://localhost:8080/api/index/sync   # 实际端口见启动日志
curl -s http://localhost:8080/api/index/status
```

> 注：`application.properties` 配置 `server.port=0`，本地运行端口见启动日志。

### 测试覆盖

| 测试类 | 场景 |
| --- | --- |
| `ReorganizationTest` | 高度 3 处分叉并延长 → 回滚 `[3,4]`、重放新链；等长分叉被拒绝且本地不变 |
| `DuplicateSyncTest` | 连续重复同步幂等、重复/冲突区块投递、8 线程并发同步（仅 1 次推进）、门闩制造在途同步验证读一致性 |
| `SourceFailoverTest` | TIMEOUT→TRANSPORT→健康源切换；三类错误全失败归因；落后源不污染；取数中途业务错误无部分写入 |

测试日志会打印：**同步水位变化**（`水位 x -> y`）、**回滚区间与哈希**
（`回滚高度区间=[from,to]`、`回滚哈希=[...]`、`重放哈希=[...]`）、
**故障来源与类别**（`数据源 [name] 同步失败：kind=TIMEOUT/TRANSPORT/BUSINESS`），
以及并发触发时的 `BUSY_SKIPPED` 计数。

## 5. 代码结构

```
model/    Block、ChainSnapshot(不可变快照)、SyncResult/SourceAttempt/SyncStatus
source/   BlockSource、LocalBlockSource(本地可注入故障的模拟源)、三类 BlockSourceException
store/    IndexStore(AtomicReference + CAS 提交，回滚/重放原子化)
service/  BlockIndexer(并发互斥、分叉检测、源探测与故障切换、可解释结果)
web/      IndexController(只读查询 + sync/投递)
config/   IndexerConfig(装配本地主/备数据源)
```
