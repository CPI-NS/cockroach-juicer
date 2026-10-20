# Juicer 规范一致性测试集：场景格式（格式 1.0 版，对应规范 1.2 版）

这份文件规定场景文件长什么样、每个字段什么意思、运行器怎么执行一个场景、怎么比较结果。三套实现（juicer-cc 的 Go 版、CockroachDB 的 Go 版、Spanner 的 C++ 版）的运行器都按这份文件读同一批场景。规范条款号指同目录上一级的 `JUICER-SPEC.md`（1.2 版）。

- 场景文件是 JSON（UTF-8），字段名用英文，说明文字用中文。
- 时间一律是微秒整数，字段名以 `_us` 结尾。
- 配置里的时长（持有上限 H、缓冲时间 W、统计周期）都取 1000 µs 的整数倍：CockroachDB 的环境变量以毫秒为单位，这样三套都能原样表达。
- 期望输出只写能从规范条文推出的东西。规范推不出或有歧义的，场景标为 `pending_ambiguity`，歧义写在 `AMBIGUITIES.md`。

## 1. 测的是什么

一个场景描述**一台服务器**上的 Juicer：热点过滤（第一级、第二级、提名与惰性移除）、每个受管 key 的队列、依赖执行器（在途持有、谁挡谁、什么事件结束持有、持有上限）、多 key 消息的整条投递、调用方放弃时的撤回，以及配置声明行与每周期摘要。被测对象之外的东西（并发控制本身、网络、客户端）都由场景的输入事件代替：消息什么时候到、并发控制什么时候回复、提交或中止什么时候执行完，都由场景写定。

场景分三层（字段 `layer`）：

| layer | 含义 | 运行器驱动什么 |
|---|---|---|
| `core` | 核心层：只依赖规范条文，与系统无关 | Juicer 核心的内部接口（队列管理器、过滤器、依赖执行器、准入调度），注入虚拟时钟 |
| `adapter` | 接入层：测各系统接入层的特有做法（表 5-C 的“执行完”事件、非事务消息、第一级中止来源、Spanner 的孤儿标记与扣留上限等） | 经过本系统的拦截器、SPI 或分类器，用构造或录下的真实消息 |
| `banner` | 只看配置声明行（与每周期摘要） | 按场景配置启动服务器或调用打印声明行的函数，截取声明行 |

## 2. 文件结构

场景按规范条款分组，每个文件一组：

```json
{
  "format": "juicer-spec-conformance",
  "format_version": "1.0",
  "spec_version": "1.2",
  "file": "s05-rules",
  "title": "§5 依赖执行器与冲突表规则（含表 5-C 各系统“执行完”事件）",
  "clauses": ["§5"],
  "scenarios": [ { ... }, { ... } ]
}
```

运行器必须拒绝 `format` 不是 `juicer-spec-conformance` 或 `format_version` 的主版本号不是 1 的文件。

## 3. 场景字段

| 字段 | 必有 | 含义 |
|---|---|---|
| `id` | 是 | 场景编号，`S<条款两位>-<序号两位>`（如 `S05-07`）；待歧义裁定的场景用 `P-<序号>` |
| `name` | 是 | 中文名称 |
| `spec_refs` | 是 | 对应的规范条款号列表 |
| `layer` | 是 | `core`、`adapter`、`banner`（第 1 节） |
| `applies_to` | 是 | 适用的系统，取值 `juicer-cc`、`crdb`、`spanner` |
| `not_applicable` | 否 | `{系统: 不适用的理由}`，`applies_to` 之外的系统都在这里写理由 |
| `status` | 是 | `active`（要跑）、`pending_caliber`（等缓冲时间口径，跳过）、`pending_ambiguity`（等歧义裁定，跳过） |
| `blocked_by` | 否 | `pending_ambiguity` 时指向 `AMBIGUITIES.md` 的编号（如 `A3`） |
| `caliber` | 否 | `pending_caliber` 时写等的是规范哪一条的什么口径 |
| `purpose` | 是 | 一句话目的 |
| `config` | 是 | Juicer 配置（第 4 节）；`banner` 层的 `{}` 表示不带任何 Juicer 参数、取代码默认 |
| `config_variants` | 否 | 同一组输入要在几种配置下各跑一遍、都得到同一个 `expect`（第 4.3 节） |
| `adapter_notes` | 否 | `{系统: 说明}`：接入层场景里抽象输入怎样对应到本系统的具体消息 |
| `inputs` | 是 | 按时间排列的输入事件（第 5 节） |
| `end_us` | 是 | 结束时刻：处理完全部输入后把时钟推到这里（触发到这一刻为止的定时器），再取最终结果 |
| `checkpoint_times` | 否 | 中途取状态快照的时刻，期望在 `expect.checkpoints` |
| `tolerance_us` | 否 | 时间比较的容差，缺省见第 7.1 节 |
| `expect` | 是 | 期望输出（第 6 节）；`pending_*` 的场景可以是 `null` |
| `expect_by_option` | 否 | 只在 `pending_ambiguity` 场景里：`{选项: {label, expect}}`，每个裁定选项下的期望；裁定后把选中的那份搬进 `expect`、状态改 `active` |
| `notes` | 否 | 补充说明 |

## 4. 配置 `config`

### 4.1 字段

| 字段 | 取值 | 缺省（= 规范 §12 默认） |
|---|---|---|
| `filter.mode` | `hotspot`：两级热点过滤决定哪些 key 有队列；`fixed`：`managed_keys` 列的 key 从时刻 0 起就有队列（已在接收状态），不经过滤、不撤、不会有 `epoch_end` | `hotspot` |
| `filter.managed_keys` | `fixed` 模式下受管的 key | — |
| `filter.K` | 第一级候选数 | 100 |
| `filter.S` | 第二级每周期提名数 | 1 |
| `filter.idle_epochs` | 连续多少个周期没有乱序到达才撤 | 3 |
| `filter.epoch_us` | 统计周期长度。**周期边界只由 `epoch_end` 输入决定**，运行器必须关掉实现自己的周期定时器；这个值只用于配置声明行，`epoch_end` 之间的间隔不必等于它 | 10 000 000 |
| `rules` | `conflict-table`（冲突表规则）或 `none`（只排序、不建依赖执行器，规范 5.5 的 NCC、MVTO） | `conflict-table` |
| `H_us` | 持有上限 H | 场景必须写（规范默认值待标定）；`banner` 层可以不写 |
| `buffer` | `{"policy": "fixed", "W_us": W}`：每个标记最早在进队时刻 + W 放行；`{"policy": "adaptive", "enabled": true/false}`：自适应缓冲的开关（打开时的口径待定，见 `pending_caliber` 场景） | `{"policy": "fixed", "W_us": 0}` |
| `late_policy` | `queue`（迟到照样进队，规范默认）或 `bypass`（非默认选项，迟到直接投递、不登记持有） | `queue` |
| `queue_removal` | `drain`（停收、按序排空、空了再删、可取消） | `drain` |
| `max_held` | 同时扣留的消息数上限（规范 6.5，可选；只有 Spanner 有） | 不设 |
| `diag` | 每消息诊断开关（规范 11.2；只有 CockroachDB 有开关） | `false` |

`config_variants` 里的 `override` 按字段覆盖 `config`；值为 `null` 表示“不写这个参数，取代码默认”。

`fixed` 模式的说明（测试集 1.1 版补）：juicer-cc 的 `--manualQueueKeys` 在某个 key 收到第一条消息时才建它的队列，不是从时刻 0 起就建。活跃场景里每个手动受管的 key 都在任何检查点之前收到过消息，所以结果不受影响；以后新写 `fixed` 模式的场景也要保证这一点，否则 `queues_now`、`queue_len` 会因实现而不同。

### 4.2 三套实现的对应参数

运行器把场景配置翻译成本实现的参数。核心层运行器通常直接构造核心对象，不经过命令行；下表给出含义相同的参数，方便对照。

| 场景字段 | juicer-cc（服务器旗子） | CockroachDB（环境变量） | Spanner（服务器旗子） |
|---|---|---|---|
| `filter.mode = fixed` + `managed_keys` | `--manualQueueKeys` | `COCKROACH_JUICER_MANUAL_QUEUE_KEYS` | `--juicer_manual_queue` + `--juicer_manual_keys`（要同时关掉在场减负 `--juicer_static_passthrough`） |
| `filter.K` / `filter.S` | `--filterK` / `--filterS` | `COCKROACH_JUICER_FILTER_K` / `_FILTER_S` | `--juicer_filter_k` / `--juicer_filter_s` |
| `filter.idle_epochs` | `--idleEpochs` | `COCKROACH_JUICER_IDLE_EPOCHS` | `--juicer_idle_epochs` |
| `filter.epoch_us` | `--epochSec`（秒） | `COCKROACH_JUICER_EPOCH_MS` | `--juicer_epoch_ms` |
| 提名时机（固定为周期末） | `--nominateMidEpoch=false` | 只有周期末 | 只有周期末 |
| 第二级从候选的同一周期开始数（2.3） | 本来如此 | 本来如此 | `--juicer_filter_watch=same-epoch` |
| `rules = conflict-table` | `--juicerRules=conflict-table`（协议取 d2PLWW、d2PLWD、OCC 等） | `COCKROACH_JUICER_RULES=conflict-table` | `--juicer_rules=conflict-table` |
| `rules = none` | 协议 NCC（或 dMVTO） | 不适用 | nccstore（`--juicer_rules` 在非 2PL 存储上解析为 off） |
| `H_us` | `--maxHoldMs`（毫秒） | `COCKROACH_JUICER_MAX_HOLD_MS`（毫秒） | `--juicer_max_hold_us` |
| `buffer.fixed W_us` | `--waitPolicy=fixed --fixedWaitUs` | `COCKROACH_JUICER_FIXED_WAIT_MS`（毫秒） | `--juicer_fixed_wait_us` |
| `buffer.adaptive enabled` | `--waitPolicy=marginal` | `COCKROACH_JUICER_AW` | `--juicer_adaptive_wait` |
| `late_policy` | `--latePolicy` | `COCKROACH_JUICER_LATE_POLICY` | `--juicer_late_policy` |
| `queue_removal = drain` | 唯一行为 | 唯一行为 | `--juicer_queue_removal=drain` |
| `max_held` | — | — | `--juicer_max_held` |
| `diag` | — | `COCKROACH_JUICER_DIAG` | — |

规范默认里其余的开关（Spanner 的中止先送、孤儿标记清除、不回复中止的释放）按代码默认（都开）跑，场景不另写。

### 4.3 配置变体

`config_variants` 是一个列表，每项 `{"name", "override", "note"?, "expect_startup"?}`。运行器对每个变体各跑一遍完整场景，每一遍都要满足同一个 `expect`；`expect_startup` 若给出，就只比较这个变体能否启动（`accept` 或 `reject`），不跑输入。报告里按 `场景 id / 变体名` 分别给结果。

## 5. 输入事件 `inputs`

每个事件都有 `t_us`（发生时刻）与 `type`。事件按 `t_us` 不减排列；同一时刻的事件按文件顺序处理。场景作者保证：输入事件与任何定时器到期时刻至少相隔 500 µs，所以结果不取决于“同一时刻输入与到期谁先”（见 `AMBIGUITIES.md` A11）。

### 5.1 `arrive`：一条数据消息到达

```json
{"t_us": 1000, "type": "arrive", "msg": "b", "txn": 2, "ts": 200,
 "ops": [{"key": "k1", "access": "write"}, {"key": "k2", "access": "read", "class": "request"}],
 "payload": "b-payload-12", "ts_by_system": {"juicer-cc": 100}}
```

| 字段 | 含义 |
|---|---|
| `msg` | 消息名，场景内唯一；所有输出都用它指代这条消息 |
| `txn` | 事务号，正整数；`0` 表示事务号未绑定（规范 5.2） |
| `ts` | 排序键。整数；或 `[wall, logical]` 两段（只用在 CockroachDB 专用场景，规范 3.1 的例外）。整数 `n` 等价于 `[n, 0]` |
| `ts_by_system` | 可选，`{系统: 排序键}`：该系统改用这个排序键。用于“同一事务的后一条消息”这类情形：juicer-cc 同一事务的排序键相同，CockroachDB 后一次发送的批头时钟更大。场景作者保证各种取值下期望相同 |
| `ops` | 这条消息碰到的 key 与访问类型；同一条消息可以多次碰同一 key（规范 9.1：合并成一个标记，类型取最强，写 > 加锁读 > 读） |
| `ops[].access` | `read`（读）、`write`（写：Set、Put、Delete、条件写、带写集的准备消息在这个 key 上的部分）、`lock_read`（读改写的读 GetForPut 与加锁读，规范 5.3 按写算） |
| `ops[].class` | 持有类别：`control`（控制类，规范 5.4 (b)）、`request`（请求类，5.4 (a)）、`any`（缺省：由本系统接入层声明；写与加锁读在三套里都是控制类，读在 juicer-cc 与 Spanner 读写事务里是控制类、在 CockroachDB 与 Spanner 只读事务里是请求类）。场景作者保证：带 `any` 的读不会遇到“回复成功”或“提交执行完”这类结果取决于类别的事件 |
| `payload` | 可选，消息内容的替身；缺省为 `msg` 的值。运行器把它放进消息，投递时核对一字不差（不变量 I2） |

`arrive` 的处理就是规范描述的到达路径：对每个碰到的 key，若它是第二级正在观察的 key，先按排序键数乱序到达（2.2）；若它有接收中的队列，判迟到（7.1）并进队（或按 `bypass` 旁路）；若它的队列在排空，直接投递、不进队（8.1）；没有队列则不产生标记。没有任何标记进队的消息立即投递。

**各系统的消息形状**（运行器据此把一条 `arrive` 变成本系统的一条消息）：

| 抽象 | juicer-cc | CockroachDB | Spanner |
|---|---|---|---|
| 一条 `arrive` | 一个 RPC，只碰一个 key（`ops` 只有一项） | 发到本节点的一个 BatchRequest，可含多个点请求；`ts` 是批头时钟 `BatchRequest.Now` | 发到本分片的一条消息：读写事务的准备消息（`ops` = 读集 + 写集），或只读事务的提交（ROCommit） |
| `read` + `control` | Get | 无 | 读写事务读集里的 key |
| `read` + `request` | 无 | 非加锁的点读（Get、点 Scan）、Refresh | 只读事务的读 |
| `read` + `any` | Get | 非加锁读 | 上面两种任选 |
| `write` | Set | Put、Delete、CPut、Increment 等 | 读写事务写集里的 key |
| `lock_read` | GetForPut（GetForUpdate RPC） | 加锁读（SELECT FOR UPDATE 的 Get/Scan） | 无 |

某个系统没有对应形状的，场景就不适用于它（写在 `not_applicable`）。

### 5.2 其余输入事件

| `type` | 字段 | 含义 |
|---|---|---|
| `reply` | `msg`、`outcome`（`ok` / `failed`） | 已投递的消息的回复从并发控制返回；`failed` 指被并发控制拒绝。多 key 消息的回复作用于它的全部 key。按持有令牌只结束这条消息自己的持有（5.4 (a)(c)） |
| `control_arrive` | `txn`、`kind`（`commit` / `abort` / `prepare`）、`keys` | 一条控制消息（提交、中止、准备）到达本服务器。它不进队，只是被观察的事件；规范 5.4 (b) 明文规定“到达”不结束任何持有 |
| `control_applied` | `txn`、`kind`（`commit` / `abort`）、`keys`、`via`（可选） | 事务 `txn` 的提交或中止在本服务器、在 `keys` 这些 key 上**执行完**（5.4 (b)、表 5-C）。若此前没有对应的 `control_arrive`，视为同一控制消息在这一刻到达并执行完 |
| `cancel` | `msg` | 调用方放弃这条消息（RPC 上下文超时或取消，10.1）。场景只在消息仍被扣着时取消；P-04 的 `after_delivery: true` 是待裁定的例外 |
| `level1_abort` | `txn`、`keys` | 第一级的输入：事务 `txn` 在这些 key 上被并发控制拒绝一次（2.1）。核心层场景里第一级**只**从这个事件取输入；运行器不得把 `reply failed` 或 `control_arrive abort` 再喂给第一级。核心层场景在同一周期里不会对同一（事务, key）重复给出，所以只接受 key 的接口（Go 的 `AbortTracker.RecordAbort(key)`、Spanner 的 `OnAbortKeys`）也能用 |
| `epoch_end` | — | 一个统计周期结束：第二级按本周期的乱序到达提名、惰性移除、清零（2.4、2.5、8.1）。周期从 0 编号，第 i 个 `epoch_end` 结束第 i−1 号周期 |
| `advance` | — | 只推进时钟（触发到期的定时器） |
| `arrive_non_txn` | `msg` | 一条非事务消息（复制、心跳一类）到达，只用在接入层场景（1.1） |

接入层场景（`layer = adapter`）里，第一级的输入来自接入层按 2.1 声明的中止来源（失败回复、带 key 的中止消息等），不再有 `level1_abort`。

两处运行器口径（测试集 1.1 版补，来自 CockroachDB 运行器）：

- **经真实拦截器跑核心层的运行器**：CockroachDB 的核心层运行器走真实拦截器，失败回复会照实现把中止报给第一级，与上面“核心层不得把 `reply failed` 喂给第一级”不同。活跃的核心层场景里，带 `reply failed` 的只有 `fixed` 模式的 S05-10、S05-11（没有第一级），所以结果相同；以后在 `hotspot` 模式的核心层场景里写 `reply failed`，要保证它是否进第一级不影响期望，或者改写成接入层场景。
- **CockroachDB 的 `control_arrive`**：CockroachDB 的一条控制消息（EndTxn、ResolveIntent）分不出“到达”与“执行完”。核心层场景里，运行器把 `control_arrive` 直接喂成核心的到达事件；接入层场景里，`control_arrive` 只能用一个回复里事务仍未决的 EndTxn 批次来表达，现在只在 S09-06 里用来报中止来源。

**`control_applied.via`**：核心层场景不写 `via`，运行器给核心喂本实现的通用“提交或中止已执行完”事件（Go 两套是 `Event{Kind: ControlApplied, OpType: OpCommit/OpAbort, TxnID}`；Spanner 是 `Event{kind: Applied, opType: Commit/Abort}`），按 `keys` 逐个 key 喂。接入层场景用 `via` 指明表 5-C 里的哪一种观测点，运行器必须走那条真实路径：

| `via` | 系统 | 含义 |
|---|---|---|
| `commit_handler_returned` / `abort_handler_returned` | juicer-cc | Commit、Abort RPC 的处理函数返回 |
| `endtxn_anchor` | crdb | 锚点 key 上 EndTxn 批次的处理函数返回 |
| `endtxn_lock_span` | crdb | EndTxn 的本地锁区间里的点 key |
| `resolve_intent` | crdb | 这个 key 的 ResolveIntent 批次处理函数返回，事件的事务号取请求里的 `IntentTxn` |
| `one_pc_reply` | crdb | 单阶段提交的写批回复，回复里事务状态为已提交 |
| `coordinator_final_reply` | spanner | 协调者的最终回复（RWCommitCoordinatorReply，先提交放锁再回复） |
| `participant_decision_applied` | spanner | 协调者的决定被本参与者的存储应用之后（DecisionIn） |
| `abort_delivered_ok` | spanner | 本事务的 Abort 或 Wound 交给存储并返回 OK |
| `noreply_abort` | spanner | 参与者在 PREPARE_WAIT 中被中止、存储不会回复（10.2） |

### 5.3 key 与事务号

- key 是 ASCII 字符串（`k1`、`k2`…）。运行器映射成本系统的 key 时必须保持字节序（规范 2.4 的平局取字节序较小者）；CockroachDB 可以在前面加同一个表前缀。
- 事务号是场景内的正整数，运行器映射成本系统的事务标识（CockroachDB 用由它确定地生成的 UUID）。

## 6. 期望输出 `expect`

运行器从被测实现里记下一份**轨迹**，再与 `expect` 比较。轨迹的内容与 `expect` 的字段一一对应。`expect` 里没写的字段不比较（接入层、配置声明行场景通常只写一部分）；写了的字段按下表比较：

| 字段 | 内容 | 比较 |
|---|---|---|
| `startup` | `accept` / `reject`：按配置能否启动 | 必须 |
| `deliveries` | 每条投递：`{msg, t_us, path}`。`path`：`queued`（这条消息至少有一个标记进过队）、`direct`（没有标记进队：碰到的 key 都不受管、队列在排空、迟到旁路、扣留上限直通）、`not_intercepted`（非事务消息）。`early: true` 表示接入层的提前投递（9.3） | 必须，全集相等；每条的时刻按 7.1；同一时刻不同消息的先后不比较 |
| `drops` | 被丢弃的消息：`{msg, t_us}` | 必须，全集相等 |
| `releases` | `{key: [{msg, t_us}, ...]}`：每个 key 的队列里标记出堆的顺序（经缓冲与依赖判定放行；被撤回的不算） | 必须，每个 key 的序列相等；排序键相等的标记之间不比较先后（3.2） |
| `holds` | 每个登记过的持有：`{msg, key, start_us, end}`。`end` 是 `"open"`（到 `end_us` 时仍在、存活不满 H），或 `{reason, t_us, lazy?}`，`reason` 取 `event`（释放事件：自己的回复、自己的失败、提交或中止执行完）、`ttl`（存活满 H）、`cancel`（调用方放弃撤回）、`queue_deleted`（队列删除时一并丢弃） | 必须，以（msg, key）为键的全集相等；见 7.2 |
| `no_hold_for` | 不得登记持有的（msg, key） | 必须 |
| `late` | 判为迟到的标记：`{msg, key, handling}`，`handling` 取 `queued` / `bypass` | 必须，全集相等 |
| `queue_events` | 队列生命周期：`{t_us, key, event}`，`event` 取 `created`、`drain_started`、`deleted`、`drain_canceled` | 必须（`hotspot` 模式），全集相等；同一 key 的事件保持先后，不同 key 同一时刻的先后不比较 |
| `epochs` | 每个周期末第二级的乱序到达计数：`{index, ooo: {key: n}}` | 必须；列出的 key 数值相等，没列出的 key 必须为 0 或不在观察之列 |
| `checkpoints` | 中途快照：`{t_us, managed, draining, queue_len, held_msgs, open_holds, counters}`。`managed` 是接收中的受管 key，`draining` 是排空中的，`queue_len` 是每个现存队列的堆长，`held_msgs` 是仍被扣着的消息，`open_holds` 是存活不满 H 的在途持有 | 必须，列出的字段逐项相等（列表按集合比）；实现里存活已满 H 但还没被清扫的持有不参与 `open_holds` 的比较 |
| `counters` | 结束时的计数（第 6.1 节） | 必须，列出的字段相等；值为 `{"min", "max"}` 时落在区间内即可 |
| `epoch_summaries` | 每周期摘要（11.1）：`{index, managed: {key: {W_us, n, r}}, created, drain_started, deleted, drain_canceled, queues_now, late_queued, hold_end, conservation}` | 必须，列出的字段相等；字段名与打印格式由运行器从本实现的摘要行解析 |
| `banner` | 配置声明行的字段（第 6.2 节） | 按第 6.2 节 |
| `buffer_timers_armed` | 场景期间为缓冲时间挂过的定时器数 | 可选：运行器观测得到才比 |

### 6.1 计数器（中性名字）

各实现的计数器名字不同，运行器负责映射。**只数 `arrive` 的数据消息**：控制消息是否计入“拦截”待裁定（A1），在那之前，若本实现的拦截数、直接投递数包含控制消息，运行器先减去它喂进去的控制消息数再比。

| 名字 | 含义（规范条款） |
|---|---|
| `intercepted`、`direct`、`queued_delivered`、`dropped`、`held_now` | 守恒计数（1.7）：拦截 = 直接投递 + 排队后投递 + 丢弃 + 仍扣着 |
| `holds_created` | 登记过的持有总数 |
| `hold_end.{event, ttl, cancel, queue_deleted}` | 按原因的持有结束次数（6.4） |
| `late_queued.{read, write, lock_read}`、`late_bypass.{...}` | 按操作类型的迟到标记数，进队与旁路分开（7.4） |
| `queues_now`、`queues_max` | 现存队列数与场景期间的最大值（2.6）；有队列在排空时不给出（A8） |
| `max_held_pass` | 因扣留上限而整条直通的消息数（6.5，仅 Spanner） |

### 6.2 配置声明行 `banner`

运行器从本实现的配置声明行（juicer-cc 的 `juicer-config:` 各行，CockroachDB 的 `juicer: spec config:`，Spanner 的 `juicer: spec: …`）解析出下列中性字段：

| 字段 | 值 |
|---|---|
| `spec_version` | 规范版本 |
| `intercepted_types` | 拦截的消息类型（1.1） |
| `level1_abort_sources` | 第一级的中止来源（2.1） |
| `K`、`S`、`epoch_us`、`idle_epochs` | 热点过滤参数 |
| `nomination` | `epoch_end` |
| `rules` | `conflict-table` / `none` |
| `hold_end_events` | `{control: [...], request: [...]}`，元素取 `applied`、`own_reply`、`own_failure`、`ttl` |
| `H_us` | 持有上限 |
| `buffer` | `{policy, W_us, adaptive}` |
| `late_policy` | `queue` / `bypass` |
| `queue_removal` | `drain` |
| `sort_key` | `txn_timestamp`（事务开始时分配的时间戳）/ `batch_clock`（CockroachDB 的批头时钟，3.1 例外） |
| `caller_abandon` | `withdraw` / `not_applicable` |
| `hygiene` | Spanner：`{abort_first, purge_orphan_markers, release_noreply_abort, max_held}` |
| `per_message_diag` | `off` / `segment_timing_only`（juicer-cc 声明过的例外） |
| `event_log` | `off` |

期望值的写法（可以出现在任意一层，例如 `hold_end_events.request`）：字面值必须相等；`{"present": true}` 只要求打印了；`{"min": n}` 要求不小于 n；`{"per_system": {系统: 值}}` 按系统取值，值为 `null` 表示该系统不打印这一项；`{"expected": v, "compare": "warn"}` 不相等只告警。

## 7. 比较规则

### 7.1 时间

- **注入时钟模式**（正式结果只认这个模式）：实际时刻必须落在 `[期望, 期望 + tolerance_us]`，`tolerance_us` 缺省 100。事件触发的输出在注入时钟下应恰好等于期望；容差只给定时器的实现细节（例如到期判定用“大于”而多等一个刻度）。
- **真实时钟模式**（还没改成可注入时钟的实现，只作参考）：运行器可以把场景里的全部时间（输入时刻、配置时长、期望时刻）乘同一个整数 f ≥ 1，容差取 `f × 100 µs + 3 ms`；报告里必须标明“真实时钟、非正式”。Go 运行时空闲时把不足 1 ms 的定时取整到约 1 ms，忙时尾部可达数毫秒（`dev/juicer-cc/STATUS.md` 的微基准），所以 f 建议取 20 以上。

### 7.2 持有的结束

- `end.reason` 必须相等。
- `end` 不带 `lazy` 时，结束时刻按 7.1 比较。这类到期（`ttl`）都发生在有队头正被它挡住的时候，按规范 6.1 必须有定时器在那一刻唤醒。
- `end` 带 `lazy: true` 的 `ttl`：那一刻没有队头在等它，实现可以晚些清扫。实际结束时刻不早于期望即可；到 `end_us` 时它若仍在在途集合里（存活已满 H、未被清扫），也算符合。对应的 `counters.hold_end.ttl` 给的是区间。
- 持有到期之后、被清扫之前又遇到自己的结束事件（回复、提交或中止执行完），结束原因记 `event` 还是 `ttl`，待裁定（`AMBIGUITIES.md` A16）。裁定之前场景作者保证不出现这种情形：每个持有要么在存活满 H 之前被事件结束，要么到期时正有队头在等它。

### 7.3 全局不变量（每个场景都查，不另写在 `expect` 里）

| 编号 | 内容 | 条款 |
|---|---|---|
| I1 | 每条 `arrive` 的消息到 `end_us` 时恰好处于三种状态之一：已投递（恰好一次）、已丢弃、仍被扣着；没有投递任何输入里没有的消息 | 1.2、1.3 |
| I2 | 投递时消息内容（`payload`）与到达时相同 | 1.2 |
| I3 | 每个在途持有都对应一个已从堆里出堆的标记（同一消息、同一 key）；堆里的标记没有持有；同一（消息, key）至多登记一个持有 | 3.3 |
| I4 | 每个检查点与结束时守恒等式成立（按第 6.1 节的口径） | 1.7 |

运行器能在每个输入之后查就每步查，至少在每个检查点与结束时查。

### 7.4 允许各实现不同的东西

下列内容不比较：计数器与日志行的名字和格式（由运行器映射）；额外的计数器；等待配对矩阵、持有寿命直方图的分桶（6.4 只要求有中位数与 p99，分位数不跨实现比较）；日志里打印的周期编号是结束的周期还是开始的周期；同一时刻不同 key 上的输出先后；同一时刻多条投递的先后；没有被挡住的持有在到期后何时被清扫（7.2）；Spanner 中止先送（L1）打开时中止消息与本事务被冲刷消息交给存储的先后。

## 8. 运行器的结果

每个（场景, 变体, 系统）一个结果：`pass`、`fail`（附逐项差异）、`skip`（附理由：`not_applicable`、`pending_caliber`、`pending_ambiguity`、`unsupported_layer`）、`error`（运行器或被测实现出错）。报告格式与接口见 `README.md` 第 4 节。

## 9. 版本

- 格式版本 `1.0`：本文件。加字段只升次版本号，运行器遇到不认识的字段必须忽略；改字段含义升主版本号。
- 测试集本身另有版本号（场景内容的版本，与格式版本无关），记在 `README.md` 末尾的变更记录里；运行器以套件摘要（`SHA256SUMS` 的 sha256）认版本。
- 场景对应的规范版本写在每个文件的 `spec_version`。规范升版本后，主会话按新条文更新场景与 `AMBIGUITIES.md`，再重新生成 `SHA256SUMS`。
