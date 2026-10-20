# Juicer 规范一致性测试集

## 1. 这是什么

用同一组场景测三套 Juicer 实现——juicer-cc 的 Go 版（juicer-grpc-go）、CockroachDB 的 Go 版（juicer-grpc-go-1572 + cockroach-juicer 的接入层）、Spanner 的 C++ 版（spanner-ncc 的 `src/store/juicer`）——证明它们对同样的输入做出同样的决定，是“同一个 Juicer”。依据是同目录上一级的 `JUICER-SPEC.md` 1.2 版（下称“规范”）。用户 10-19 同意做这个测试集（原话“可以有”，规范拍板第 10 条）：主副本放在这里，各仓库放逐字节相同的副本，测试启动时核对 sha256。

每个场景是一台服务器上的一小段历史：Juicer 的配置，一串带逻辑时间（微秒）的输入事件（消息到达、回复、提交或中止到达与执行完、调用方取消、第一级的中止、周期边界、时钟推进），以及期望的输出（什么消息在什么时刻以什么路径投递、每个 key 的出堆顺序、哪些持有在何时以何种原因结束、哪些标记判为迟到、队列何时建立与撤销、每周期的乱序计数、计数器与守恒等式、配置声明行）。期望只取规范条文推得出的东西；推不出的列在 `AMBIGUITIES.md`。

这个目录只有场景与格式。三套的运行器由各自的开发子代理以后写（第 4、5 节是给他们的约定）。

## 2. 目录

```
spec-conformance/
  README.md            本文件：是什么、场景一览、运行器约定、三套怎么接、怎么维护
  FORMAT.md            场景格式：字段、输入事件语义、期望字段、比较规则、容差、不变量
  AMBIGUITIES.md       规范推不出期望的 17 处（A14 已定），各自的建议与牵涉的场景
  SHA256SUMS           除它自己以外每个文件的 sha256（sha256sum 格式，路径相对本目录）
  scenarios/
    s01-conservation.json   §1 拦截与消息守恒
    s02-filter.json         §2 热点过滤
    s03-ordering.json       §3 排序
    s04-buffer.json         §4 缓冲时间（口径相关的标 pending_caliber）
    s05-rules.json          §5 冲突表规则，含表 5-C 各系统“执行完”事件
    s06-hold-cap.json       §6 持有与上限
    s07-late.json           §7 迟到消息
    s08-lifecycle.json      §8 队列生命周期
    s09-multikey.json       §9 多 key 消息与重复访问
    s10-cancel.json         §10 超时与取消
    s11-banner.json         §11 配置声明行与每周期摘要（§12 默认值）
    p-pending-ambiguity.json 待歧义裁定的场景（运行器跳过）
```

## 3. 场景一览

共 77 个：活跃 68 个，等缓冲时间口径 2 个（`pending_caliber`），等歧义裁定 7 个（`pending_ambiguity`）。

| 条款 | 活跃 | 待定 | 其中适用于三套 / 两套 / 一套（活跃） |
|---|---|---|---|
| §1 拦截与守恒 | 3 | — | 2 / 1 / 0 |
| §2 热点过滤 | 12 | — | 10 / 0 / 2 |
| §3 排序 | 5 | — | 5 / 0 / 0 |
| §4 缓冲时间 | 3 | 2（口径） | 3 / 0 / 0 |
| §5 依赖执行器与规则表 | 19 | — | 7 / 6 / 6 |
| §6 持有与上限 | 4 | — | 1 / 2 / 1 |
| §7 迟到 | 4 | — | 3 / 0 / 1 |
| §8 队列生命周期 | 4 | — | 4 / 0 / 0 |
| §9 多 key 与重复访问 | 7 | — | 0 / 5 / 2 |
| §10 超时与取消 | 3 | — | 0 / 1 / 2 |
| §11 声明行与摘要 | 4 | — | 3 / 0 / 1 |
| 待歧义裁定 | — | 7 | — |
| 合计 | 68 | 9 | 38 / 15 / 15 |

按系统数活跃场景：CockroachDB 59 个，Spanner 51 个，juicer-cc 49 个（S04-03 有 3 个配置变体、S11-04 有 2 个，运行器报告里的条数按变体计）。两套的 15 个里，juicer-cc + CockroachDB 8 个（同一事务多条消息、加锁读、取消、事务号未绑定），CockroachDB + Spanner 6 个（多 key 消息），juicer-cc + Spanner 1 个（只排序的协议）；一套的 15 个是 CockroachDB 7 个（排序键两段时钟、EndTxn/ResolveIntent/1PC、多 key 取消、诊断开关）、Spanner 6 个（参与者与协调者的执行完、中止交付、不回复的中止、孤儿标记、扣留上限）、juicer-cc 2 个（Commit 处理函数、同一事务同时到达）。

分层：核心层 55 个、接入层 11 个、配置声明行 2 个（活跃）。

| 编号 | 名称 | 条款 | 层 | 适用 | 状态 |
|---|---|---|---|---|---|
| S01-01 | 三种结局各出现一次，守恒等式每步成立 | 1.2、1.3、1.7、10.1 | core | jcc、crdb | active |
| S01-02 | 没有取消时的守恒：直接投递与排队后投递 | 1.2、1.3、1.7 | core | jcc、crdb、sp | active |
| S01-03 | 非事务消息不进扣留路径、不计入拦截 | 1.1、1.7 | adapter | jcc、crdb、sp | active |
| S02-01 | 候选从同一周期开始被数乱序，周期末才建队列 | 2.2、2.3、2.4、8.1 | core | jcc、crdb、sp | active |
| S02-02 | 没有乱序到达不提名 | 2.2、2.4 | core | jcc、crdb、sp | active |
| S02-03 | 排序键相等不算乱序到达（不同事务） | 2.2、3.1 | core | crdb | active |
| S02-04 | 乱序次数相同取 key 字节序较小者 | 2.4 | core | jcc、crdb、sp | active |
| S02-05 | 只提名乱序最多的前 S 个（S = 2） | 2.4、2.6 | core | jcc、crdb、sp | active |
| S02-06 | 连续 3 个周期没有乱序到达才撤，空队列立即删除 | 2.5、8.1 | core | jcc、crdb、sp | active |
| S02-07 | 空闲期间出现一次乱序到达则空闲计数清零；迟到标记也计入乱序 | 2.2、2.5、7.1、7.2 | core | jcc、crdb、sp | active |
| S02-08 | 掉出前 S 不撤；S = 1 时两个队列并存到旧的空闲满 3 周期 | 2.4、2.5、2.6 | core | jcc、crdb、sp | active |
| S02-09 | 乱序到达看到达过的最大排序键，迟到看已放行的最大排序键 | 2.2、4.2、7.1 | core | jcc、crdb、sp | active |
| S02-10 | 第一级只取中止最多的前 K 个（K = 2） | 2.1、2.3 | core | jcc、crdb、sp | active |
| S02-11 | t_max 每个周期清零 | 2.2 | core | jcc、crdb、sp | active |
| S02-12 | CockroachDB：排序键按（物理, 逻辑）比较，逻辑部分更小也算乱序 | 2.2、3.1 | core | crdb | active |
| S03-01 | 按排序键出堆 | 3.2、3.5、4.2 | core | jcc、crdb、sp | active |
| S03-02 | 被挡多次的队头不会被强行放出 | 3.4、6.1 | core | jcc、crdb、sp | active |
| S03-03 | 登记的持有就是被放出的那个标记 | 3.3、3.2 | core | jcc、crdb、sp | active |
| S03-04 | 一次唤醒放完全部可放的标记 | 3.6 | core | jcc、crdb、sp | active |
| S03-05 | 放行时刻是“缓冲到点”与“依赖允许”二者取晚 | 3.5、4.2 | core | jcc、crdb、sp | active |
| S04-01 | W 从每个标记自己的进队时刻起算 | 4.2 | core | jcc、crdb、sp | active |
| S04-02 | 缓冲期内到达的更小排序键先放 | 4.2、3.2 | core | jcc、crdb、sp | active |
| S04-03 | W = 0 退化为只排序加依赖执行器；默认就是 W = 0 | 4.4、4.5、12.1 | core | jcc、crdb、sp | active |
| S04-04 | 缓冲时间上限大于持有上限时拒绝启动 | 4.3、6.3 | core | jcc、crdb、sp | pending_caliber |
| S04-05 | 自适应缓冲：每周期、每个受管 key 一个 W，周期内不变 | 4.1、4.6 | core | jcc、crdb、sp | pending_caliber |
| S05-01 | 读读不挡 | 5.3 | core | jcc、crdb、sp | active |
| S05-02 | 写挡读（不同事务） | 5.3、5.4(b) | core | jcc、crdb、sp | active |
| S05-03 | 读挡写（不同事务），没有任何事件时持有到期放行 | 5.3、5.1、6.1 | core | jcc、crdb、sp | active |
| S05-04 | 写挡写（不同事务） | 5.3、5.4(b) | core | jcc、crdb、sp | active |
| S05-05 | 读改写的读与加锁读按写算 | 5.3、表5-A 语义冲突 | core | jcc、crdb | active |
| S05-06 | 同一事务的标记不互挡，控制类持有在提交执行完时一起结束 | 5.3、9.4(b)、9.4(c) | core | jcc、crdb | active |
| S05-07 | 控制类持有：回复返回不结束、提交到达不结束、提交执行完才结束 | 5.4(b)、表5-A 控制冲突 | core | jcc、crdb、sp | active |
| S05-08 | 控制类持有：中止到达不结束，中止执行完结束 | 5.4(b) | core | jcc、crdb、sp | active |
| S05-09 | 请求类持有在自己的回复返回时结束 | 5.4(a)、表5-B | core | crdb、sp | active |
| S05-10 | 持有者自己的请求失败即结束 | 5.4(c) | core | jcc、crdb、sp | active |
| S05-11 | 失败只结束失败的那个请求自己的持有 | 5.4(c)、9.4(c) | core | jcc、crdb | active |
| S05-12 | 事务号未绑定的队头放行但不登记持有 | 5.2 | core | jcc、crdb | active |
| S05-13 | 只排序的协议（NCC、MVTO）不建依赖执行器 | 5.5、5.1、3.2、4.2 | core | jcc、sp | active |
| S05-14 | juicer-cc：Commit 处理函数返回才结束控制类持有 | 5.4(b)、表5-C | adapter | jcc | active |
| S05-15 | CockroachDB：锚点上的 EndTxn 与 ResolveIntent 各自结束持有，ResolveIntent 事件的事务号取 IntentTxn | 5.4(b)、表5-C | adapter | crdb | active |
| S05-16 | CockroachDB：单阶段提交的回复（事务已提交）结束持有 | 5.4(b)、表5-C | adapter | crdb | active |
| S05-17 | Spanner 参与者：投票回复不结束，决定被存储应用后结束 | 5.4(b)、表5-C | adapter | sp | active |
| S05-18 | Spanner 协调者：最终回复结束持有 | 5.4(b)、表5-C | adapter | sp | active |
| S05-19 | Spanner：中止交给存储并返回 OK 后结束持有 | 5.4(b)、表5-C | adapter | sp | active |
| S06-01 | 被多个持有挡住时，最后一个挡人的持有到期才放 | 6.1 | core | jcc、crdb、sp | active |
| S06-02 | 两个事务在两个受管 key 上互相挡住，持有到期打破（多 key 消息） | 6.1、§6 跨 key 互等、9.2、7.2 | core | crdb、sp | active |
| S06-03 | 两个事务在两个受管 key 上互相挡住，持有到期打破（单 key 消息） | 6.1、§6 跨 key 互等、7.2 | core | jcc、crdb | active |
| S06-04 | Spanner：同时扣留的消息数达到上限后，新消息整条直通 | 6.5 | adapter | sp | active |
| S07-01 | 迟到的写照样进队、等在途持有、放行时登记持有，t_h 不回退 | 7.1、7.2、7.4 | core | jcc、crdb、sp | active |
| S07-02 | 迟到的读也进队；默认配置下旁路计数为 0 | 7.2、7.4 | core | jcc、crdb、sp | active |
| S07-03 | 排序键与 t_h 相等不算迟到（不同事务） | 7.1、3.2 | core | crdb | active |
| S07-04 | 非默认的旁路选项：迟到标记直接投递、不登记持有 | 7.3、7.4 | core | jcc、crdb、sp | active |
| S08-01 | 撤队列：先停收，已排队的按序经依赖判定放出，堆空即删 | 8.1、6.4 | core | jcc、crdb、sp | active |
| S08-02 | 排空时不一次放光：已排队的照样等缓冲时间 | 8.1、4.2 | core | jcc、crdb、sp | active |
| S08-03 | 排空中被重新提名：取消删除、保留持有、恢复进队 | 8.2、2.3、2.4 | core | jcc、crdb、sp | active |
| S08-04 | 删除后重新提名：新队列从空开始（新的 t_h、没有旧持有） | 8.1、7.1、2.4 | core | jcc、crdb、sp | active |
| S09-01 | 多 key 消息的全部标记都放行才整条投递 | 9.1、9.2 | core | crdb、sp | active |
| S09-02 | 先放行的标记的持有从它自己放行时计时 | 9.2、6.1 | core | crdb、sp | active |
| S09-03 | 同一消息对同一 key 的读与写合并成一个写标记 | 9.1 | core | crdb、sp | active |
| S09-04 | 不受管的 key 不产生标记 | 9.1、9.2 | core | crdb、sp | active |
| S09-05 | juicer-cc：同一事务两条消息同时访问同一 key | 9.4(b)、9.4(c)、9.4(d)、7.1 | core | jcc | active |
| S09-06 | 第一级按“事务 × key”计一次中止 | 9.4(d)、2.1 | adapter | jcc、crdb | active |
| S09-07 | Spanner：终止消息冲刷本事务被扣的消息，留下的孤儿标记被清除 | 9.3、1.2、1.6 | adapter | sp | active |
| S10-01 | 排队中被调用方放弃：标记撤回、不投递、计入丢弃 | 10.1、1.3、1.7 | core | jcc、crdb | active |
| S10-02 | 多 key 消息被放弃：撤回未放行的标记，撤销已登记的持有并唤醒队列 | 10.1、9.2 | core | crdb | active |
| S10-03 | Spanner：参与者在 PREPARE_WAIT 中被中止、不回复，也要结束持有 | 10.2、5.4(b) | adapter | sp | active |
| S11-01 | 不带任何 Juicer 参数启动：配置声明行等于规范默认 | 11.1、12.1、1.1、1.6、2.1、11.2、11.3 | banner | jcc、crdb、sp | active |
| S11-02 | 显式配置如实反映在配置声明行 | 11.1、2.7、6.2、7.3 | banner | jcc、crdb、sp | active |
| S11-03 | 每周期摘要的字段与取值 | 11.1、1.7、2.6、6.4、7.4 | core | jcc、crdb、sp | active |
| S11-04 | CockroachDB：每消息诊断开与关，决策序列逐条相同 | 11.2 | core | crdb | active |
| P-01 | 上一周期的第一级候选在下一周期是否仍被观察 | 2.1、2.3 | core | jcc、crdb、sp | 待 A3 |
| P-02 | 排空中的 key 不是第一级候选时是否继续被第二级观察 | 2.3、8.2 | core | jcc、crdb、sp | 待 A5 |
| P-03 | 未绑定事务号的队头遇到冲突的持有 | 5.2 | core | jcc、crdb | 待 A6 |
| P-04 | 投递之后调用方才放弃：已登记的持有撤不撤 | 10.1、1.3 | core | jcc、crdb | 待 A9 |
| P-05 | 控制消息是否计入拦截数与直接投递数 | 1.7、1.1 | core | jcc、crdb、sp | 待 A1 |
| P-06 | 同时存在的队列数是否包括正在排空的队列 | 2.6、8.1 | core | jcc、crdb、sp | 待 A8 |
| P-07 | CockroachDB：EndTxn 锁区间里、属于本节点其他 range 的 key，何时算提交执行完 | 5.4(b)、表5-C | adapter | crdb | 待 A15 |

（jcc = juicer-cc，crdb = CockroachDB，sp = Spanner。）

**运行结果**（10-20，测试集 1.0 版，注入时钟）：juicer-cc 通过 50、失败 1（S11-01 的 `hold_end_events.request`，是场景的问题，1.1 版已改）；Spanner 通过 54、失败 0；CockroachDB 通过 62、失败 0。报告在 `dev/<系统>/conformance-report*.jsonl`。1.0 版时预期 S02-12 在 CockroachDB 上失败（第二级只比较批头时钟的物理部分），CockroachDB 已改为按完整排序键判断，现在通过。

**测试集覆盖不到的条款**（要靠各仓库自己的测试或冒烟）：1.4 投递原子、1.5 只读并发控制状态（代码评审）；3.3 判定与出堆之间的并发竞争（场景是顺序执行的，只能查顺序下的不变量 I3；竞争要靠 `-race` 下的并发压测，规范 §3 验收）；4.4 后半句“自适应关闭时不建统计对象”（看不到）；4.6 公式本身（口径定了另加 `s04-formula.json`）；6.2 H 的标定（要跑实验）；6.4 寿命分位数（各实现分桶不同，不跨实现比）；2.6 队列数的时间加权平均；§12 负载生成器不去重（不属 Juicer 核心）。

## 4. 运行器约定

每个仓库写一个运行器，读本目录的场景、驱动本实现的 Juicer、比较结果、出报告。比较规则以 `FORMAT.md` 为准，这里只写运行器的接口与流程。

### 4.1 启动核对

1. 运行器源码里写死**套件摘要**：本目录 `SHA256SUMS` 文件自身的 sha256。
2. 启动时先算副本里 `SHA256SUMS` 的 sha256，与套件摘要比；再按 `SHA256SUMS` 逐个核对每个文件。任何一处不符就整体失败，不跑任何场景（防止有人只改了某个仓库的副本）。
3. 只认 `format_version` 主版本号为 1 的文件。

### 4.2 每个场景怎么跑

```
对每个场景 s、s 的每个配置变体 v（没有变体就是一遍）：
  若本系统不在 s.applies_to、或 s.status 不是 active、或本运行器还不支持 s.layer：记 skip（附理由）
  h = 新建一个 Juicer（一台服务器），配置 = s.config 覆盖 v.override，注入虚拟时钟
      启动失败 → 结果里 startup = reject；只比较 startup
  对 s.inputs 里的每个事件 ev（按 t_us、同一时刻按文件顺序）：
      h.AdvanceTo(ev.t_us)        先把时钟推到 ev.t_us：按时间顺序触发到期的定时器，每触发一个就跑到静止
      h.Apply(ev)                 按 FORMAT 第 5 节的语义喂给实现
      h.Quiesce()                 等实现内部的 goroutine 或事件循环处理完
      查不变量 I1–I4（至少在检查点与结束时）
      到了 s.checkpoint_times 里的时刻：h.AdvanceTo(t)，h.Snapshot()
  h.AdvanceTo(s.end_us)
  trace = h.Trace()
  diffs = Compare(s.expect, trace, s.tolerance_us 或 100)
```

`h` 至少要提供这些操作（名字随语言，含义固定）：

| 操作 | 作用 |
|---|---|
| `AdvanceTo(t)` | 推进虚拟时钟到 t，按到期先后触发定时器（缓冲时间到点、持有到期） |
| `Arrive(msg, txn, ts, ops, payload)` | 一条数据消息到达，走完整到达路径（第二级计数 → 迟到判定 → 进队或直通 → 放行判定）；记下它最终的投递或丢弃 |
| `Reply(msg, outcome)` | 回复事件，带这条消息在每个 key 上的持有令牌 |
| `ControlArrive(txn, kind, keys)`、`ControlApplied(txn, kind, keys, via)` | 控制消息到达、执行完 |
| `Cancel(msg)` | 结束这条消息的 RPC 上下文 |
| `Level1Abort(txn, keys)`、`EpochEnd()` | 第一级的输入、周期边界（运行器自己的周期定时器必须关掉） |
| `NonTxn(msg)` | 接入层场景：一条非事务消息 |
| `Snapshot()` | 受管与排空中的 key、各队列堆长、仍扣着的消息、存活不满 H 的持有、守恒计数 |
| `Trace()` | 投递、丢弃、每个 key 的出堆序列、持有的登记与结束、迟到判定、队列生命周期、每周期乱序计数、计数器、每周期摘要、配置声明行（解析成 FORMAT 第 6.2 节的中性字段） |

### 4.3 报告

- 每个（场景, 变体）一行 JSON：`suite_digest`、`format_version`、`spec_version`、`system`、`repo`、`commit`、`clock_mode`（`injected`，或 `real` 加放大倍数 f）、`scenario`、`variant`、`result`（`pass` / `fail` / `skip` / `error`）、`diffs`（每项 `{field, expected, actual}`）、`skip_reason`。
- 最后一行汇总：各结果的个数、失败与出错的场景编号。
- 有任何 `fail` 或 `error` 时退出码非零。真实时钟模式的结果在汇总里单独标出，不算正式结果。
- 报告与运行记录写在 `docs/eval-plan-20261019/dev/<系统>/` 下（不写 memory/）。
- 运行器发现场景本身可能有错时，先对照规范条文；确认是场景的问题就报给主会话，由主会话改主副本，**不要直接改仓库里的副本**。

## 5. 三套怎么接

下面的入口取自三份开发记录（`dev/<系统>/CHANGES.md`、`STATUS.md`）与各分支头的代码，只指路，不规定写法。

### 5.1 juicer-cc（juicer-grpc-go `spec-align-20261019` 28e3bfcb；juicer-cc 052809c）

- **核心对象**（`juicer-grpc-go-align/juicer/`）：`QueueManager`（`NewQueueManager`、`addQueue`、`EnqueueAdmission`、`ObserveForKeys`、`Summary`）、`PerKeyQueue`（`Enqueue`、`Cancel`、`Counters`、排空的 `beginDrain`/`CancelDrain`）、`DependencyEnforcer`（`OnHeadEligible`、`ObserveAt`、`CancelHold`、`Stats`）、`TxnDispatcher.AwaitAdmission(ctx, ops)`（取消与守恒计数）、`AbortTracker.RecordAbort`、`ReorderAnalyzer`（`RecordOperation`、`ApplyDelta`、`SetEpochEndHook`、`EpochKeyStats`）。
- **冲突表规则在 juicer-cc 仓库里**：`server/server.go` 的 `conflictTableRules(maxHold)`（未导出；规则名常量 `RulesConflictTable`，测试 `server/rules_test.go`），协议到规则的选择也在 `server/server.go`，NCC、dMVTO 不建依赖执行器。所以核心层运行器适合写成 juicer-cc 仓库 `server` 包的包内测试，引用 juicer-grpc-go 的导出接口；需要的内部观测点（出堆、持有登记与结束）在 juicer-grpc-go 加可选的观察者接口，CockroachDB 谱系已有 `SetAdmissionObserver`、`SetHoldReleaseObserver` 可以照搬。
- **可照着写的现有测试**：`juicer/per_key_queue_ordering_test.go`（3.3、3.4）、`per_key_queue_late_test.go`（§7）、`queue_lifecycle_test.go`（§8）、`filter_epoch_test.go`（2.3）、`dependency_cancel_test.go`（§10）；根包的 `juicer_control_applied_test.go`（接入层 S05-14：真 gRPC 服务器、拖慢 Commit 处理函数）、`juicer_banner_test.go` 的 `TestBannerAndEpochSummary`（配置声明行 `juicer-config:`、每周期 `juicer-epoch:`）。
- **时钟**：取时与定时器直接用 `time.Now`、`time.NewTimer`，没有可注入时钟。规范“验收工具”一节估计先做约 100 行重构（取时与定时器经一个时钟接口，不改决策）；在那之前只能跑真实时钟模式（FORMAT 第 7.1 节）。
- **测试门**：`ASDF_GOLANG_VERSION=1.26.5`，`go test -race` 整包、每次新进程（`memory/code-branches-and-build.md` 第 4 节的坑）。

### 5.2 CockroachDB（juicer-grpc-go-1572 `spec-align-20261019` 1db0402b；cockroach-juicer 15ca45a6788）

- **核心层**（`crdb-align/juicer-grpc-go-1572/juicer/`，包内测试）：`ConflictTableRules`（`dependency_rules.go`）、`QueueManager`（`EnqueueAdmission` 带逻辑时钟、`SetAdmissionObserver`、`SetHoldReleaseObserver`，正好用来记出堆与持有结束）、`TxnDispatcher.AwaitAdmission` 与 `Conservation()`（守恒计数）、`ReorderAnalyzer`（`SnapshotMetrics`）、排空（`BeginDrain`/`CancelDrain`）、迟到档。可照着写：`conflict_table_test.go`、`reorder_analyzer_spec_test.go`、`queue_drain_test.go`、`late_policy_test.go`、`observability_test.go`、`dependency_cancel_test.go`。测试门：`crdb-align/build/forktest.sh`。
- **接入层与配置声明行**（`crdb-align/cockroach-juicer/pkg/rpc/juicer_spi.go`）：`SplitMarker`（拆标记、同 key 合并取强）、`AppliedControlEvents`（EndTxn 锚点与锁区间、ResolveIntent 按 `IntentTxn`、单阶段提交）、`BuildRules`、配置声明行 `juicer: spec config:` 与每周期 `epoch summary:`。S05-15、S05-16、S09-06、S01-03、S11-01、S11-02 在这里跑，用构造或录下的真实批次；测试门：bazel `//pkg/rpc:rpc_test`（`crdb-align/build/build.sh test`），副本要在 BUILD 里列为 data。
- **时钟**：同 juicer-cc，Go 核心没有可注入时钟，要先做同样的重构。
- 场景里的 `ts` 映射成批头时钟 `(wall = ts, logical = 0)`；`[wall, logical]` 两段的只在 CockroachDB 专用场景里出现。

### 5.3 Spanner（spanner-ncc `spec-align-20261019` 5195e69）

- **核心层**（`ncc-port/spanner-ncc-align/src/store/juicer/`）：`QueueManager` 用 `QueueManagerOptions` 构造（热点或手动选择、`rules = BuildRules(RulesMode::ConflictTable, H)`、`filterK`、`filterS`、`idleEpochs`、`latePolicy`、`drainStopsAdmission`、`clock`、`timers`、`sink`），输入走 `OnAbortKeys`（第一级）、`OnHoldCandidateKey`（第二级到达计数）、`EnqueueOp(Marker)`、`ObserveForKey(s)`（`EventKind::RespReturned`、`Applied`）、`RunEpoch(&HotspotEpochReport)`；出堆由 `MarkerSink::OnMarkerReleased` 报告。**时钟已可注入**：`clock.h` 的 `Clock`、`TimerSource`，测试用 `tests/fakes.h` 的 `FakeClock`、`FakeTimers`、`FakeTimerSource`。可照着写：`tests/spec-drain-test.cc`、`spec-filter-test.cc`、`spec-conflict-table-test.cc`、`spec-config-test.cc`。
- **多 key 消息、整条投递、取消以外的接入层场景**（§9、S06-02、S06-04、S09-07、S10-03、表 5-C 的 S05-17 至 S05-19）要驱动 `core.{h,cc}` 而不只是 `QueueManager`：用黄金台（`tests/golden-test.cc`、`golden-scenarios.h`、`fake-2pl-store.h`）那套假传输与假存储。
- **读 JSON 有先例**：`tests/engine-vectors-test.cc` 已用 nlohmann/json 读 `engine-vectors.json`。
- **配置声明行**：`core.cc` 的 `juicer: spec: version = …` 与 `juicer: epoch-summary: …`；`dev/spanner/cli-smoke.sh` 演示了启动单进程服务器截取横幅。
- **测试门**：`dev/spanner/build-native.sh` 构建，`dev/spanner/run-tests.sh` 跑（ASan 构建在 labpc 上要 `setarch -R`，脚本已处理）。

## 6. 怎么维护

- **主副本只在这里**。改场景、格式或歧义清单都只改这个目录，再同步到各仓库；仓库里的副本一律不直接改。
- **各仓库的副本位置**（建议，以运行器所在的包为准，落定后记在 `dev/<系统>/`）：juicer-cc 仓库 `server/testdata/spec-conformance/`（或运行器所在包的 `testdata/`）；juicer-grpc-go-1572 `juicer/testdata/spec-conformance/`；cockroach-juicer `pkg/rpc/testdata/spec-conformance/`；spanner-ncc `src/store/juicer/tests/spec-conformance/`。
- **改动流程**：
  1. 改 `scenarios/*.json`（或 FORMAT、AMBIGUITIES）。每个改动的场景，期望都要能指到规范条文；推不出的写进 `AMBIGUITIES.md`、状态标 `pending_ambiguity`。
  2. 检查 JSON 能解析：`for f in scenarios/*.json; do python3 -m json.tool "$f" > /dev/null || echo "$f"; done`。
  3. 重新生成校验和（在本目录下）：`find . -type f ! -name SHA256SUMS -printf '%P\n' | LC_ALL=C sort | xargs sha256sum > SHA256SUMS`（路径不带 `./` 前缀；核对用 `sha256sum -c SHA256SUMS`）。
  4. 算新的套件摘要：`sha256sum SHA256SUMS`。
  5. 把整个目录逐字节复制到每个副本位置（`rsync -a --delete --checksum spec-conformance/ <副本位置>/`），在各仓库里把运行器源码的套件摘要改成新值，作为同一个提交。
  6. 三个运行器各跑一遍，结果记在 `dev/<系统>/`。
- **规范升版本时**：按新条文核对每个场景的 `spec_refs` 与期望，更新文件头的 `spec_version`；歧义裁定后把 `expect_by_option` 里选中的那份搬进 `expect`、状态改 `active`，并在 `AMBIGUITIES.md` 里把该条标“已裁定（日期、结果）”，不删。
- **缓冲时间口径定下后**：启用 S04-04（按 4.3 的结果改期望）、补全 S04-05，另加 `scenarios/s04-formula.json`（公式的表驱动用例，以 Spanner `adaptive-wait-test` 的用例为底）。

## 7. 来历

- 2026-10-19 至 10-20，Opus 5.5 子代理（模型标识 claude-opus-5-5）按规范 1.2 版与三份开发记录写成；只写了本目录，没有改任何代码仓库、没有跑实验。
- 期望先按规范条文手算；再用一个只用于核对的参考模型（Python，不属于本测试集）逐个跑过：手算与模型一致；活跃场景在 A3、A5、A6、A7 各选项的全部组合下输出相同；输入事件与任何定时器到期至少相隔 500 µs；带 `ts_by_system` 的场景在各系统的取值下期望相同。
- 10-20 按三个运行器的报告出测试集 1.1 版（见下面的变更记录）。参考模型同时改为按 A16 的建议先把过期未清扫的持有记为到期、再处理事件，并检查活跃场景里不出现 A16 的情形；全部场景重新生成后，除变更记录列出的改动外，场景文件逐字节不变。

## 8. 变更记录

测试集版本是场景内容的版本，与 `FORMAT.md` 的格式版本（仍为 1.0）无关；运行器以套件摘要（`SHA256SUMS` 的 sha256）认版本。

| 测试集版本 | 日期 | 套件摘要（前 16 位） | 改了什么 | 原因 |
|---|---|---|---|---|
| 1.0 | 2026-10-19 至 10-20 | c72757d5e18507956 | 初版：77 个场景（活跃 68、等口径 2、等歧义 7），AMBIGUITIES 15 条 | 规范 1.2 版，拍板第 10 条 |
| 1.1 | 2026-10-20 | 不写在这里：README 本身在 SHA256SUMS 里，写进来会改变摘要；以 `sha256sum SHA256SUMS` 为准 | 见下面的清单 | 三个运行器 10-20 的报告与各自 STATUS.md 的“场景问题”，主会话逐条裁定 |

测试集 1.1 版的改动：

1. **S11-01**：`banner.hold_end_events.request` 改为按系统给：juicer-cc 为 `null`（冲突表规则下 juicer-cc 全是控制类持有，与 S05-09 对 juicer-cc 的不适用理由一致），CockroachDB 与 Spanner 仍为 `[own_reply, own_failure, ttl]`。`spec_version` 由“期望 1.2、只告警”改为严格等于 `"1.2"`（A14 已定：三套声明行统一印规范版本号）。原因：juicer-cc 运行器报的唯一失败是场景写错；A14 由主会话裁定。
2. **S05-12**：`applies_to` 去掉 Spanner，`not_applicable` 写明理由（Spanner 的事务号 0 是真实事务号，没有“未绑定”的概念，核心的未绑定路径由它自己的 engine-vectors 覆盖）；格式不加新标志。**P-03** 同一理由一并去掉 Spanner。原因：Spanner 运行器报告无法表达。
3. **S02-06、S02-07、S02-08**：H 由 10 ms 调到 200 ms，每个持有都在到期前被自己的事件结束；输入与期望的放行、提名、撤销时刻不变，期望里只有 `config.H_us` 变了，另加了 `notes`。原因：Spanner 运行器指出原来的持有在到期之后、清扫之前被事件放掉，结束原因算 event 还是 ttl 有歧义；这一条记为 AMBIGUITIES **A16**。
4. **AMBIGUITIES.md**：A14 标“已定”；新加 **A16**（上一条）与 **A17**（CockroachDB 的队列 key 是原 key 的 FNV-64a 哈希，2.4 的平局在真实部署里比的是哈希；选项是改 CockroachDB 用原 key、或规范对 CockroachDB 写明例外，待用户定；测试集没有覆盖它的接入层场景）。A6 补记 Spanner 没有未绑定的概念。共 17 条。
5. **FORMAT.md**（不改字段、不改已有场景的期望，格式版本仍为 1.0）：第 4.1 节补 juicer-cc 的手动受管 key 在第一条消息到达时才建队列；第 5.2 节补 CockroachDB 运行器的两处口径（核心层经真实拦截器跑时失败回复会报给第一级，现有带 `reply failed` 的核心层场景都在 `fixed` 模式，不受影响；CockroachDB 一条控制消息分不出到达与执行完，接入层的 `control_arrive` 只能用回复里事务未决的 EndTxn 表达，只在 S09-06 用）；第 6.2 节写明期望值的写法可以出现在任意一层；第 7.2 节写明 A16 裁定前场景避开那种情形；第 9 节说明测试集版本。
6. **README.md**：场景一览与计数随第 2 条更新（活跃场景适用于三套 / 两套 / 一套为 38 / 15 / 15，Spanner 51 个），记下 10-20 的运行结果，加本节。

