# Reorg-Safe Block Indexer（重组安全的区块索引服务）

一个基于 Spring Boot 3 / Java 21 的本地区块索引服务。区块来源**仅**来自本地进程内的模拟数据源
（`InMemoryBlockSource`）或仓库已有依赖，**不访问外部网络、不使用任何真实凭据**。

服务在链重组、重复同步、数据源超时 / 传输故障 / 业务错误等情况下，始终维护一个
**对外可查询的、完整一致的规范链视图**，并对失败给出可解释、可区分的结论。

---

## 1. 能力总览

| 能力 | 说明 |
| --- | --- |
| 增量同步 | 从数据源拉取区块并维护规范链视图，同步可被并发、重复触发 |
| 链重组处理 | 检测分叉 → 计算回滚点 → 丢弃失效后缀 → 应用新链，全程原子发布 |
| 多数据源 | 按优先级 / 能力选择数据源，超时、传输故障自动切换，跨源结果交叉校验 |
| 失败可解释 | 区分 `TIMEOUT` / `TRANSPORT` / `BUSINESS` / `NO_SOURCE`，携带每个源的尝试记录 |
| 幂等投递 | 重复同步、重复投递同一区块不会产生重复数据，水位不会倒退 |
| 无锁读 | 查询永远读到「上一个」或「下一个」完整快照，同步进行中也能稳定读取 |

---

## 2. 一致性模型

### 2.1 不可变快照 + 原子发布（Copy-On-Write）

- 规范链视图是不可变值对象 `ChainState`（record），内部区块列表高度连续、父哈希闭合。
- 存储层 `InMemoryIndexStore` 只持有一个 `volatile ChainState` 引用：
  - 读取方**无锁**读取当前快照；
  - 同步方在锁内先完成所有拉取、校验、分叉点计算和新链组装，构造好**完整**的新快照后，
    通过一次 volatile 写（`publish`）原子替换引用。
- 因此查询在任何时刻只会看到「上一个完整快照」或「下一个完整快照」，
  **绝不会看到正在回滚或写入一半的状态**。旧快照会一直服务已开始的读取直到自然结束。

### 2.2 同步串行化与水位单调

- 所有写操作（`syncOnce` / `ingest`）由同一把 `ReentrantLock` 串行化，
  并发触发的同步排队执行：恰好一次真正应用，其余识别为 `duplicate`。
- 同步水位（`SyncWatermark`：最高确认高度 + 尖端哈希）只随成功发布的快照推进：
  - 数据源链头落后于本地水位 → 拒绝，返回 `BUSINESS`；
  - 任何拉取失败、结构非法、跨源分歧 → 不发布，水位保持旧值；
  - 与当前链头一致 → 幂等 no-op。
  所以**水位高度在任何失败路径下都不会倒退**。

### 2.3 链重组算法

设本地规范链为 L，数据源链头为 H：

1. 拉取候选链 0..H 的全部区块（批量优先，失败退化为逐高度故障切换拉取）；
2. 校验候选链：创世父哈希为 null、高度连续、逐块父哈希闭合；
3. 从 `min(L.tip, H)` 向下找**最高的同高度同哈希点**作为分叉点 F；
   - 找不到任何共同祖先（连创世都不同）→ 拒绝，返回 `BUSINESS`，本地状态不动；
4. 回滚范围 = `[F+1, L.tip]`（仅日志与结果中可见，内存里直接丢弃旧后缀，不产生中间态）；
5. 新链 = `L[0..F] + 候选[F+1..H]`，再次校验连续性与父哈希链接；
6. 一次性发布新快照。

日志示例：

```
WARN  REORG detected via local-primary: rollback heights [2, 4] (3 blocks) before applying new chain
INFO  sync committed via local-primary: watermark 4@A-h4 -> 4@A-fork7-h4, applied heights [2, 4], rollback [2, 4]
```

### 2.4 重复投递（消息语义）

`POST /api/blocks` / `BlockIndexerService.ingest`：

- 区块哈希已在规范链中 → `duplicate=true`，忽略；
- 高度 = tip+1 且数据源交叉确认哈希一致 → 延伸；
- 高度 > tip+1（有缺口）→ `BUSINESS` 失败，不写入；
- 与已确认区块同高度但哈希不同 → 作为重组候选走 2.3 的完整流程。

---

## 3. 数据源兼容与切换规则

### 3.1 数据源抽象

`BlockSource` 是唯一的数据来源接口，内置实现为进程内模拟源 `InMemoryBlockSource`：

- `setChain(...)`：切换链视图，模拟源端重组；
- `failOnceTimeout / failOnceTransport / failOnceBusiness / failHead*`：
  按高度或链头注入一次性故障；
- 全部行为即时发生在本地内存中（超时不真实等待），无网络、无凭据。

每个源通过 `SourceCapabilities(name, maxHeight, batchFetchSupported, priority)` 声明能力。

### 3.2 选择与切换规则（`SourceRouter`）

1. **能力过滤**：目标高度超过源 `maxHeight`、或需要批量但源不支持批量时，该源不参与；
   没有任何合格源时结论为 `NO_SOURCE`（HTTP 503）。
2. **优先级**：合格源按 `priority` 升序尝试。
3. **故障切换**：
   - `SourceTimeoutException` → `TIMEOUT`（HTTP 502）：切换下一个源；
   - `SourceTransportException` → `TRANSPORT`（HTTP 502）：切换下一个源；
   - `SourceBusinessException` → `BUSINESS`（HTTP 409）：也尝试下一个源做交叉验证。
4. **主导失败类型**：全部候选源都失败时，按尝试记录归类（同数时 `BUSINESS > TRANSPORT > TIMEOUT`），
   `SyncResult.attemptedSources` 与 API 的 `sources` 字段逐个给出**来源名、类型、原因**。
5. **跨源一致性**：同步前聚合所有源报告的链头：
   - 选择**最高高度**的链头作为候选（滞后源不会把同步高度拉低）；
   - 同一最高高度若不同源哈希不一致 → `BUSINESS` 拒绝，本地状态完全不动；
   - 仅有部分源暂时不可用（超时/传输）不阻塞其它健康源。
6. **失败不留部分写入**：区块级拉取在任一中途失败都会放弃本次同步，
   因为发布动作只发生在新链完整构造并校验通过之后。

### 3.3 失败结论示例

```json
{
  "error": "TIMEOUT",
  "message": "sync failed (TIMEOUT) across 2 source(s); local state untouched, watermark stays <empty:-1>",
  "sources": [
    { "sourceName": "local-primary",   "type": "TIMEOUT",   "message": "simulated timeout ... at head" },
    { "sourceName": "local-secondary", "type": "TIMEOUT",   "message": "simulated timeout ... at head" }
  ]
}
```

---

## 4. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `GET  /api/chain` | 当前规范链快照（`tipHeight` / `tipHash` / `blocks`） |
| `GET  /api/watermark` | 当前同步水位 |
| `GET  /api/blocks/{height}` | 按高度查询区块，不存在返回 404 |
| `GET  /api/blocks/hash/{hash}` | 按哈希查询区块 |
| `GET  /api/blocks?from=&to=` | 高度闭区间查询 |
| `POST /api/sync` | 触发一次同步；成功返回 `SyncResult`，失败返回 4xx/5xx + 可解释错误体 |
| `POST /api/blocks` | 投递单个区块（JSON 同 `Block`），重复投递幂等 |

`SyncResult` 字段：`success, watermark, previousWatermark, reorgRollbackFromHeight,
appliedFromHeight, appliedToHeight, sourceName, failureType, failureMessage,
attemptedSources, duplicate, detail`。

---

## 5. 本地验证方法

要求：JDK 21+、Maven 3.9+（依赖均取自本地 `~/.m2` 缓存，可离线构建）。

```bash
# 离线编译
mvn -o compile

# 离线执行全部测试（19 个用例）
mvn -o test

# 只跑某一类场景
mvn -o test -Dtest=ChainReorgTest          # 链重组：回滚 + 重新应用 + 无共同祖先拒绝
mvn -o test -Dtest=DuplicateSyncTest       # 重复同步/重复投递/16 线程并发同步 + 并发只读
mvn -o test -Dtest=SourceFailoverTest      # 超时/传输切换、全部失败可解释、跨源分歧、能力过滤
mvn -o test -Dtest=ChainQueryControllerTest# REST 接口冒烟与失败状态码
```

测试控制台会打印：

- **同步水位**：`sync start: watermark before = ...`、`sync committed ... watermark X -> Y`；
- **回滚范围**：`REORG detected ... rollback heights [from, to]`；
- **故障来源**：`sync source failure: source=..., type=TIMEOUT|TRANSPORT|BUSINESS, detail=...`。

### 覆盖场景对照

| 需求场景 | 对应用例 |
| --- | --- |
| 分叉后回滚失效数据并应用新链 | `ChainReorgTest.rollsBackForkedBlocksAndAppliesNewChain`、`deepReorgReplacesTip` |
| 重组期间 / 同步进行中查询一致 | 上述用例的快照完整性断言、`DuplicateSyncTest.concurrentSyncKeepsSingleConsistentState` 中的并发只读线程 |
| 重复同步 / 重复投递幂等、水位单调 | `repeatedSyncIsIdempotent`、`duplicateDeliveryIsIdempotent`、`deliveryOfNextBlockExtendsChain` |
| 缺口投递被拒绝且无部分写入 | `gappedDeliveryIsRejectedWithoutPartialWrite` |
| 超时 / 传输故障切换 | `SourceFailoverTest.timeoutFailsOverToSecondary`、`transportFailureFailsOverToSecondary` |
| 全部失败原因可区分、无部分写入 | `allSourcesFailWithExplainableConclusion` |
| 数据源结果 / 高度不一致不污染状态 | `divergentSourcesDoNotPolluteLocalState`、`capabilityLimitedSourceIsSkipped` |

---

## 6. 代码结构

```
domain/   Block, SyncWatermark, ChainState          不可变领域模型与快照
source/   BlockSource, SourceCapabilities,          数据源抽象、三类异常、本地模拟实现
          InMemoryBlockSource, *Exception
store/    InMemoryIndexStore                         volatile 快照原子发布
service/  SourceRouter, BlockIndexerService,        故障切换、同步/重组/幂等核心
          SyncResult, SourceFailure, FailureType
web/      ChainQueryController, ApiError             REST 接口
config/   IndexerConfig, LocalSourcesConfig          Spring 装配（两个本地模拟源）
```
