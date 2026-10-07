# Reorg-Safe Block Indexer（防重组区块索引服务）

一个基于 Spring Boot 3 / Java 21 的本地区块索引服务。区块数据**只来自进程内模拟源**
（`LocalBlockSource`），不依赖任何外部网络、第三方节点或真实凭据。服务保证在
**链重组、同高分歧、同水位链切换、重复同步、并发触发与数据源故障**下，
索引状态始终一致、水位单调、失败可解释。

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

1. **取数阶段**：从每个最高高度候选源整链取回 `[0, head]`，在内存中校验高度连续、
   父子相接，再做跨源共识判定（见 1.4）；
2. **提交阶段**：只有在来源达成唯一共识后，`IndexStore.commit` 才在私有草稿上完成
   「回滚失效段 + 拼接新段」，再用 **CAS** 整体替换快照引用。

取数阶段抛出的任何错误（超时 / 传输 / 业务 / 链不自洽）都发生在写入之前，已取回的区块
随草稿一起丢弃；来源之间分歧时同样不进入提交，**本地状态零改动**。

### 1.3 链重组（reorg）规则

同步时从本地水位沿高度向下与**来源共识链**逐块比对哈希（纯追加、延长重组与等长链切换
都走同一条路径）：

- 找到第一个哈希相同的高度 `h`，则分叉点 `forkHeight = h + 1`；创世即分叉则为 0；
- 回滚 `[forkHeight, oldTip]` 的全部区块，重放共识链上 `[forkHeight, newTip]` 的区块；
  等长链切换时两段长度相同，水位不变；
- 回滚与重放在**同一次原子提交**内完成，提交结果中包含 `rollbackFromHeight`、
  `rolledBackHashes`、`appliedHashes`，并在日志中打印回滚区间。

### 1.4 来源同高分歧判定（不盲选任何一边）

多个来源都可用、链头高度也相同时，服务**不会随便挑一个来源**，而是：

1. 从每个最高高度候选源整链取回 `[0, head]`，先在内存校验高度连续、父子相接；
2. 按**每个高度的哈希序列**对来源分组：两个来源只有在**所有高度**上哈希逐一相同，
   才算“同一条链”（只比对 tip 不够——tip 相同但深处不同的畸形链会被全长校验识别，
   不自洽者按 BUSINESS 排除）；
3. 取回成功的来源若落入**多于一个组**，即来源之间在同一高度给出了不同链：
   - 返回 `SOURCES_DIVERGED`，**不采用任何来源，本地一个字都不改**，水位保持；
   - 结果中给出按配置顺序排列的逐源明细，`failureKind=DIVERGENCE`，
     `message` 标明**首个分歧高度**与各自 tip，便于人读与机器区分；
4. 待这些来源重新对得上（重新同步时哈希序列一致），下一次同步即正常推进，服务不会被卡住。

同样的分歧反复出现时，每次结论都是确定性的：首个分歧高度、逐源明细、`message` 完全一致，
不产生任何回滚/写入，已到达的水位绝不后退。

### 1.5 同水位/更深位置的链切换（等长链按重组处理）

本地已索引到某个高度后，来源在**同一高度、甚至更低位置**给出另一条不同的链，
不再被当成“已同步”跳过，而是按一次**链切换（reorg）**处理：

- 从本地水位沿高度向下与**来源共识链**逐块比对哈希定位共同祖先（分叉点 `forkHeight`）；
- **原子地**回滚 `[forkHeight, oldTip]` 作废段、按顺序重放共识链上的对应区段；
- 等长切换时回滚块数 == 重放块数，**水位不增不减**；新链更长时水位上升，
  状态分别为 `REORGANIZED` / `ADVANCED`；
- 整个切换通过一次快照 CAS 完成，对外查询**要么看到切换前完整的老链，
  要么看到切换后完整的新链**，绝不会看到半截状态；
- 兼容边界（长度相同怎么处理）：
  - 来源共识链与本地**等长且完全一致** → `ALREADY_SYNCED`（幂等，零回滚零重放）；
  - 来源共识链与本地**等长但内容不同** → `REORGANIZED` 原子切换，水位不变；
  - 来源共识链**严格短于**本地（所有来源一致落后）→ 拒绝并归为 `BUSINESS`，
    返回 `ALL_SOURCES_UNAVAILABLE`，因为接受它会导致水位倒退；
  - 本地为空、来源也为空 → `ALREADY_SYNCED`。

### 1.6 水位单调

- 索引水位（已提交链头高度）**只增不减**：纯追加上升，等长链切换保持不变，
  严格变短的替代链被拒绝；
- 源高度落后于最高高度的其它源 → 视为能力不足，拒绝并切换下一个候选；
- 存储层对「高度不连续 / 未接在回滚点之后 / 提交导致水位倒退」直接抛异常兜底。

### 1.7 并发与重复投递

- `sync()` 与 `submitBlock()` 共用一把 `ReentrantLock`；
- `sync()` 使用 `tryLock`：并发触发时只有一个真正执行，其余立即返回 `BUSY_SKIPPED`（不排队、不重复写）；
  分歧状态下同样适用——同一时刻最多一次真正评估；
- 重复同步返回 `ALREADY_SYNCED`；重复投递同一区块返回 `ALREADY_SYNCED`，不产生重复行；
- 乱序 / 缺口 / 孤块 / 同高度冲突哈希的投递返回 `REJECTED`，**不产生任何写入**
  （分叉重放只能通过 `sync` 完成，避免单块投递绕过重组/共识检测）；
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
| `SourceBusinessException` | `BUSINESS` | 源可达但数据不被接受：区块缺失、高度字段不一致、源落后、链不自洽、内容损坏 |
| —（来源间） | `DIVERGENCE` | 仅出现在同步结果 `attempts` 中：同高度来源之间哈希序列不一致（见 1.4） |

### 2.3 探测、取数与共识顺序

1. **链头探测**：先向所有源调用 `headHeight()`。探测失败按上表分类记录，源被移出候选；
2. **最高高度为准**：比较成功源的高度，落后于最高高度的源记为 `BUSINESS`（能力不足）——
   不同源高度不一致时，低高度源的数据**不会**被采用，避免污染；
3. **整链取数**：对每个最高高度候选源整链取回 `[0, head]` 并校验高度连续、父子相接；
   某个源取数中途失败（超时 / 断连 / 高度 3 损坏 / 链不自洽），该源被移出共识判定，
   其余源继续——**绝不拼接来自不同源的区块**；
4. **跨源共识**：取回成功的源按全高度哈希序列分组（见 1.4）：
   - 多个组 → `SOURCES_DIVERGED`，不采用任何一边，本地不动；
   - 唯一组 → 该链为“来源共识链”，再与本地比对，按 1.3 / 1.5 推进或原子切换；
   - 全部取数失败 → `ALL_SOURCES_UNAVAILABLE`；
5. **全部不可用**：结果中 `attempts` 给出每个源的
   `sourceName / success / failureKind / detail`（`DIVERGENCE` / `TIMEOUT` /
   `TRANSPORT` / `BUSINESS`），本地水位与链保持不变。

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
`REJECTED` / `SOURCES_DIVERGED` / `ALL_SOURCES_UNAVAILABLE`。

各状态含义：

| status | 含义 |
| --- | --- |
| `ADVANCED` | 纯追加推进到新水位 |
| `REORGANIZED` | 链切换/重组：回滚作废段并重放（含等长链原子切换，水位不变） |
| `ALREADY_SYNCED` | 与来源共识等长且完全一致，幂等跳过 |
| `BUSY_SKIPPED` | 已有同步在进行，本次并发触发被干脆跳过 |
| `REJECTED` | 单块投递不合法，未产生写入 |
| `SOURCES_DIVERGED` | 同高度来源之间内容互相矛盾，无法判定规范链，本地保持不动 |
| `ALL_SOURCES_UNAVAILABLE` | 探测/取数全部失败，或来源一致链严格短于本地（拒绝水位倒退） |

---

## 4. 本地验证方法

要求 JDK 21+、Maven 3.9+（已验证可离线构建）。

```bash
# 编译
mvn -o compile

# 运行全部单测（无需网络；Spring 上下文测试 + 四组核心场景，共 19 个用例）
mvn -o test

# 只跑同高分歧/链切换这一组
mvn -o test -Dtest=SameHeightDivergenceTest

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
| `ReorganizationTest` | 高度 3 处分叉并延长 → 回滚 `[3,4]`、重放新链；**同水位等长分叉按链切换原子回滚/重放（水位不变）、切换后幂等** |
| `DuplicateSyncTest` | 连续重复同步幂等、重复/冲突区块投递、8 线程并发同步（仅 1 次推进）、门闩制造在途同步验证读一致性 |
| `SourceFailoverTest` | TIMEOUT→TRANSPORT→健康源切换；三类错误全失败归因；落后源不污染；取数中途业务错误无部分写入 |
| `SameHeightDivergenceTest` | ①两源同高度不同链→`SOURCES_DIVERGED` 且本地不动；②已索引后分歧保持原样、恢复一致后推进；③多源一致的同水位链原子切换（含更低位分叉）；④同一分歧重复 5 次结论稳定、零写入、水位不退、恢复后不卡；⑤分歧下 6 线程并发仅 1 次真正评估；⑥取数中途超时切换健康源、分歧叠加第三源损坏仍判分歧；⑦tip 相同深处不同被全长校验识别；⑧共识链严格更短被拒绝且水位不退 |

测试日志会打印：**同步水位变化**（`水位 x -> y`）、**回滚区间与哈希**
（`回滚高度区间=[from,to]`、`回滚哈希=[...]`、`重放哈希=[...]`）、
**同高分歧**（`数据源之间同高分歧：首个分歧高度=h，水位保持=w`）、
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
