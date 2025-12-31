# 压缩/降级/遗忘策略表 + SLO 护栏（窗口、Top-K、TTL、超限处理）

---

# 一、全局 SLO 护栏（必须先拍板）

这是你们系统的**硬指标**，建议在 v1 就内建监控与告警。

## 1.1 核心性能 SLO

| 指标 | 目标值（默认） | 说明 |
| --- | --- | --- |
| `build_memory_packet P50` | ≤ 30 ms | 本地 SQLite / 单 agent |
| `build_memory_packet P95` | ≤ 80 ms | 可感知但不影响交互 |
| `build_memory_packet P99` | ≤ 150 ms | **硬红线** |
| `total_candidate_count` | ≤ 100 | 所有记忆类型合计 |
| `per_memory_type_candidate` | ≤ 30 | facts / episodes / procedures / insights |
| `active_facts_count` | ≤ 200 | 超出即视为治理失败 |
| `unvalidated_insight_in_responder` | 0 | **绝对禁止** |

> 说明
> 
> - 这些不是“建议”，而是**系统必须强制执行的上限**
> - 超限 → 立即触发 **更激进的压缩/降级/遗忘策略**

---

# 二、Recall 总控策略（全局）

### Recall 顺序（不可改变）

1. **Working Memory（WM）** → 永远完整
2. **Facts（active + valid）**
3. **Procedures（task_type Top-K）**
4. **STM Summary + Key Quotes**
5. **Episodes（时间窗 + cue 激活）**
6. **Insights（planner only）**

### Recall 总护栏（伪规则）

```
IF total_candidate_count > MAX_TOTAL:
    tighten_time_window()
    reduce_top_k()
    increase_compression_level()
    drop_low_confidence()
    record_in_explain()

```

---

# 三、分类型策略表（核心）

下面是你要的**压缩 / 降级 / 遗忘策略表**，按记忆类型拆分。

---

## 3.1 Working Memory（WM）——不压缩，只裁剪表示

> 定位：注意力本身
> 
> 
> **原则**：不能丢，只能“换表达形式”
> 

| 项目 | 策略 |
| --- | --- |
| 窗口 | run 级（唯一） |
| Top-K | N/A |
| TTL | run 生命周期 |
| 压缩 | ❌ 不压缩 |
| 降级 | ✅ plan/decisions 可从详细 → 简要 |
| 遗忘 | run_end 自动清空 |
| 超限处理 | 若 WM 过大：1️⃣ 移除历史决策细节2️⃣ 只保留当前 goal/constraints/slots |
| SLO 影响 | WM 必须 O(1) 读取 |

---

## 3.2 Short-Term Memory（STM）

### 3.2.1 Conversation Window

| 项目 | 策略 |
| --- | --- |
| 窗口 | 最近 N 轮（默认 N=5） |
| Top-K | N |
| TTL | session |
| 压缩 | N 轮外 → rolling_summary |
| 降级 | responder 默认 **不注入** window |
| 遗忘 | session_end |
| 超限处理 | 立即缩到 N=3 |
| 说明 | window 是“昂贵资源”，摘要才是常态 |

### 3.2.2 Rolling Summary / Key Quotes

| 项目 | 策略 |
| --- | --- |
| 窗口 | session |
| Top-K | key_quotes ≤ 10 |
| TTL | session |
| 压缩 | 句级摘要（LLM/规则） |
| 降级 | quotes → 引用 ref |
| 遗忘 | session_end |
| 超限处理 | key_quotes 降到 ≤5 |

---

## 3.3 Semantic Facts（语义事实，最关键）

> 这是长期性能的生死线
> 

### Facts 主规则（必须）

| 规则 | 强制性 |
| --- | --- |
| 每个 `fact_key` 仅允许 1 条 `active` | ✅ |
| `deprecated / disputed` 默认不参与 recall | ✅ |
| 过期（valid_to）不参与 recall | ✅ |

### Facts Recall 策略表

| 项目 | 策略 |
| --- | --- |
| 窗口 | 无时间窗（由状态控制） |
| Top-K | ≤ 30 |
| TTL | 无（由 validity 管理） |
| 压缩 | 多事实 → 合并为规范表述 |
| 降级 | value → short_value |
| 遗忘 | 仅通过治理（soft/hard delete） |
| 超限处理 | 1️⃣ 按 task relevance 排序2️⃣ 低 relevance 不注入 |
| SLO 护栏 | `active_facts_count > 200` → **告警** |

> 工程含义
> 
> 
> Facts 的 recall 规模应 ≈ `O(#keys)`，而不是 `O(#events)`。
> 

---

## 3.4 Episodic Memory（经历记忆，必须压缩）

> 这是记忆“无限增长”的关键缓冲层
> 

### 时间线压缩策略（硬规则）

| 时间距离 | 表示形式 | 参与 Recall |
| --- | --- | --- |
| ≤ 7 天 | raw episode | ✅ |
| 7–30 天 | phase_summary | ⚠️（按 cue） |
| 30–90 天 | milestone_summary | ⚠️（强 cue） |
| > 90 天 | theme_summary | ❌（默认） |

### Episodes 策略表

| 项目 | 策略 |
| --- | --- |
| 窗口 | 默认 30 天 |
| Top-K | ≤ 20 |
| TTL | 无（但被压缩） |
| 压缩 | raw → phase → milestone → theme |
| 降级 | summary → highlights → ref |
| 遗忘 | 可选：theme_summary 超 1 年 |
| 超限处理 | 1️⃣ 缩时间窗2️⃣ 提升 compression_level |
| SLO 护栏 | episode_candidate > 30 → 强制压缩 |

---

## 3.5 Procedural Memory（程序性记忆）

> 这是“会不会越用越聪明”的来源
> 

### Procedures 策略表

| 项目 | 策略 |
| --- | --- |
| 窗口 | task_type |
| Top-K | ≤ 5 / task_type |
| TTL | 无 |
| 压缩 | 多策略 → 抽象步骤 |
| 降级 | detailed steps → outline |
| 遗忘 | usage_count 低 + 长期未命中 |
| 超限处理 | 按 priority/usage 排序 |
| 注入限制 | responder 默认不注入 |

---

## 3.6 Insight（灵感 / 假设，强约束）

> Insight 是最危险的记忆类型
> 

### Insight 生存规则（必须）

| 规则 | 强制性 |
| --- | --- |
| 默认 TTL = run | ✅ |
| 未验证不得进入 responder | ✅ |
| 未晋升不得进入长期 recall | ✅ |

### Insight 策略表

| 项目 | 策略 |
| --- | --- |
| 窗口 | run |
| Top-K | ≤ 10 |
| TTL | run / 短 TTL |
| 压缩 | statement 级 |
| 降级 | 不存在（直接过期） |
| 遗忘 | run_end 自动 |
| 超限处理 | 按 confidence 丢弃 |
| SLO 护栏 | insight_count > 10 → 丢弃 |

---

# 四、统一“超限处理阶梯”

当 **任何 recall 阶段超限**，必须按顺序执行：

```
Level 1: Reduce Top-K
Level 2: Tighten Time Window
Level 3: Increase Compression Level
Level 4: Drop Low Confidence / Low Relevance
Level 5: Omit Entire Memory Type (record explain)

```

并且必须：

- **写入 `budget_report.degradations`**
- **写入 `explain.omitted`**

---

# 五、必须落地的运行期指标（Instrumentation）

你们的 Rust core 至少要暴露以下指标：

### Recall 结构性指标

- `candidate_count_by_type`
- `compression_level_distribution`
- `degradation_events_count`

### 性能指标

- `build_memory_packet_latency_ms{p50,p95,p99}`
- `db_query_time_ms_by_stage`

### 治理健康指标

- `active_facts_count`
- `episode_raw_count_over_30d`
- `insight_expired_rate`

> 一旦这些指标异常增长，系统就是“在变慢的路上”
> 

---

# 六、可以直接写进技术方案的结论段

> 我们采用类人记忆的工程实现：
> 
> 
> **记忆并非被“搜索”，而是被“线索激活”。**
> 
> 系统通过严格的候选集上限、时间窗、Top-K、压缩与 TTL，确保即使长期记忆规模持续增长，
> 
> **每一次上下文装配的计算复杂度与延迟仍保持稳定、可预测。**
> 

---

##