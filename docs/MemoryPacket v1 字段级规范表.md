# MemoryPacket v1 字段级规范表

---

# MemoryPacket v1 字段级规范表

## 1) 顶层结构

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `meta` | object | Y | — | P/T/R | (composer生成) | 元信息与预算。决定整包语义。 |
| `short_term` | object | Y | — | P/T/R | wm_state + stm_summary (+events) | 运行态与短期连续性。 |
| `long_term` | object | Y | — | P/T/R | facts + episodes + procedures | 长期稳定内容。 |
| `insight` | object | Y | `{usage_policy:{allow_in_responder:false}, hypotheses:[], strategy_sketches:[], patterns:[]}` | P(可注入)/T(一般不)/R(默认不) | insights | 候选假设层，默认不进入 responder。 |
| `citations` | array | Y | `[]` | P/T/R | events + tool_results refs | 统一证据引用清单（用于可追溯与调试）。 |
| `budget_report` | object | Y | (composer生成) | P/T/R | context_builds | 预算使用与降级记录。 |
| `explain` | object | Y | (composer生成) | P/T/R | context_builds | 选择/过滤/裁剪原因、冲突提示。 |

> 注：P=planner、T=tool、R=responder。注入模板指“允许注入到 LLM 上下文”，并不等同于“字段存在”。
> 

---

## 2) meta（元信息）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `meta.schema_version` | string | Y | `"v1"` | P/T/R | 常量 | schema 版本；破坏性变更升级。 |
| `meta.scope` | object | Y | — | P/T/R | 调用入参 | 作用域隔离：tenant/user/agent/session/run。 |
| `meta.scope.tenant_id` | string | N | `"default"` | P/T/R | 调用入参/配置 | 多租户可选。 |
| `meta.scope.user_id` | string | Y | — | P/T/R | 调用入参 | 必填。 |
| `meta.scope.agent_id` | string | Y | — | P/T/R | 调用入参 | 必填。 |
| `meta.scope.session_id` | string | Y | — | P/T/R | 调用入参 | 会话线程。 |
| `meta.scope.run_id` | string | Y | — | P/T/R | 调用入参 | 单次执行/图 run。 |
| `meta.generated_at` | string(ISO8601) | Y | now() | P/T/R | composer生成 | 生成时间。 |
| `meta.purpose` | enum | Y | — | P/T/R | 调用入参 | planner/tool/responder。影响注入规则与过滤。 |
| `meta.task_type` | string | N | `"generic"` | P/T/R | 调用入参/WM | 用于 procedure 选择与规则。 |
| `meta.cues` | object | N | `{}` | P/T/R | 调用入参/WM | 明确线索：tags/entities/keywords/time_range。 |
| `meta.budget` | object | Y | (policy默认) | P/T/R | policy配置 | token预算（总+分区）；可运行期覆盖。 |
| `meta.budget.max_tokens` | int | Y | policy默认 | P/T/R | policy配置 | 全局预算。 |
| `meta.budget.per_section` | object | Y | policy默认 | P/T/R | policy配置 | 分区预算（见 budget_report 对应）。 |
| `meta.policy_id` | string | N | `"default"` | P/T/R | 调用入参/配置 | 便于回放/A-B。 |

---

## 3) short_term（短期：WM + STM）

### 3.1 short_term 顶层

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `short_term.working_state` | object | Y | `{}` | P/T/R | wm_state | WM：目标/计划/槽位/约束/工具证据摘要。 |
| `short_term.rolling_summary` | string | Y | `""` | P/T/R | stm_summary | 会话滚动摘要；长度受 budget 控制。 |
| `short_term.key_quotes` | array | N | `[]` | P/T/R | stm_summary (+events) | 关键原句引用：必须带 evidence_id。 |
| `short_term.conversation_window` | array | N | `[]` | P/T/R | events | 最近 N 轮消息（可选；默认只给摘要+引用）。 |
| `short_term.open_loops` | array | N | `[]` | P/T/R | wm_state (+events) | 未完成事项/待确认问题；影响 planner。 |
| `short_term.last_tool_evidence` | array | N | `[]` | P/T | wm_state (+events/tool results) | 最近关键工具输出引用（不贴全量）。 |

> 注入建议：
> 
> - responder：优先 `rolling_summary + key_quotes`，避免注入过长 conversation_window。
> - tool：优先 `working_state.constraints + last_tool_evidence`。
> - planner：完整使用 `working_state + open_loops`。

### 3.2 short_term.working_state（结构化约束）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `short_term.working_state.goal` | string | N | `""` | P/T/R | wm_state | 当前目标（单句）。 |
| `short_term.working_state.plan` | array | N | `[]` | P/T | wm_state | 步骤列表；responder 通常不需要全量。 |
| `short_term.working_state.slots` | object | N | `{}` | P/T/R | wm_state | 结构化变量（客户名、日期、参数等）。 |
| `short_term.working_state.constraints` | object | N | `{}` | P/T/R | wm_state | 硬约束：must_include/must_avoid/tone/format等。 |
| `short_term.working_state.tool_evidence` | array | N | `[]` | P/T | wm_state | 工具证据引用（ref+summary），不含全量 payload。 |
| `short_term.working_state.decisions` | array | N | `[]` | P/T/R | wm_state (+events) | 已做决定（可回指证据）。 |
| `short_term.working_state.risks` | array | N | `[]` | P/T | wm_state | 风险与缓释（多用于 planner/tool）。 |
| `short_term.working_state.state_version` | int | Y | 0 | P/T/R | wm_state | 用于回放与一致性（乐观锁）。 |

### 3.3 short_term.key_quotes（关键引用）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `key_quotes[].evidence_id` | string | Y | — | P/T/R | events | 引用来源事件 ID（不可缺）。 |
| `key_quotes[].quote` | string | Y | — | P/T/R | events | 原句内容（可裁剪）。 |
| `key_quotes[].role` | enum | N | `"user"` | P/T/R | events | user/assistant/tool。 |
| `key_quotes[].ts` | string | N | — | P/T/R | events | 时间戳（可选）。 |

---

## 4) long_term（长期：facts/procedures/episodes）

### 4.1 long_term 顶层

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `long_term.facts` | array | Y | `[]` | P/T/R | facts (+fact_evidence) | 稳定事实/偏好/规则（默认只取 active+valid）。 |
| `long_term.preferences` | array | N | `[]` | P/T/R | facts | 可选分组（本质是 facts 子集）；便于模板化。 |
| `long_term.procedures` | array | Y | `[]` | P/T | procedures | 程序性策略（task_type 驱动）；responder 一般不需要全量。 |
| `long_term.episodes` | array | Y | `[]` | P/T/R | episodes | 经历摘要（默认按时间窗/标签激活，小集合）。 |

> 默认选择规则（写入 policy）：
> 
> - facts：`status=active` 且 `valid_to is null or now<=valid_to`。
> - procedures：匹配 `task_type`，取 Top-K（priority/usage）。
> - episodes：按 time_window + tags/entities 激活，并做压缩。

### 4.2 facts（事实/偏好/规则）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `facts[].fact_id` | string | Y | — | P/T/R | facts | 主键，用于引用/纠错。 |
| `facts[].fact_key` | string | Y | — | P/T/R | facts | 规范化 key（或 subject |
| `facts[].value` | any | Y | — | P/T/R | facts | JSON/文本均可；模板层负责格式化。 |
| `facts[].status` | enum | Y | `"active"` | P/T/R | facts | active/disputed/deprecated。默认只注入 active。 |
| `facts[].validity.valid_from` | string | N | — | P/T/R | facts | 有效期起。 |
| `facts[].validity.valid_to` | string/null | N | null | P/T/R | facts | 有效期止；过期不注入。 |
| `facts[].confidence` | number | N | 0.5 | P/T/R | facts | 置信度（工具证据可更高）。 |
| `facts[].sources` | array | Y | `[]` | P/T/R | fact_evidence | 证据引用（event/tool ids）。 |
| `facts[].scope_level` | enum | N | `"user"` | P/T/R | facts | user/agent/tenant，用于冲突与继承。 |
| `facts[].notes` | string | N | `""` | P/T/R | facts | 可选说明（非必须注入）。 |

### 4.3 procedures（程序性策略）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `procedures[].procedure_id` | string | Y | — | P/T | procedures | 主键。 |
| `procedures[].task_type` | string | Y | — | P/T | procedures | 任务类型键。 |
| `procedures[].content` | object | Y | — | P/T | procedures | 步骤/工具偏好/输出格式等。 |
| `procedures[].priority` | int | N | 0 | P/T | procedures | Top-K 选择依据。 |
| `procedures[].sources` | array | N | `[]` | P/T | procedures (+events) | 证据/来源（可选）。 |
| `procedures[].applicability` | object | N | `{}` | P/T | procedures | 适用范围（标签/角色/条件）。 |

### 4.4 episodes（经历摘要）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `episodes[].episode_id` | string | Y | — | P/T/R | episodes | 主键。 |
| `episodes[].time_range.start` | string | Y | — | P/T/R | episodes | 时间范围起。 |
| `episodes[].time_range.end` | string | N | start | P/T/R | episodes | 时间范围止。 |
| `episodes[].summary` | string | Y | — | P/T/R | episodes | 摘要（压缩后的主要载体）。 |
| `episodes[].highlights` | array | N | `[]` | P/T/R | episodes | 高亮要点（更短）。 |
| `episodes[].tags` | array | N | `[]` | P/T/R | episodes | 标签（用于 cue activation）。 |
| `episodes[].entities` | array | N | `[]` | P/T/R | episodes | 实体（客户/项目/系统等）。 |
| `episodes[].sources` | array | Y | `[]` | P/T/R | episode_evidence or episodes | 证据引用（event/tool）。 |
| `episodes[].compression_level` | enum | N | `"raw"` | P/T/R | episodes | raw/phase_summary/milestone。 |
| `episodes[].recency_score` | number | N | — | P/T/R | composer计算 | 时间衰减评分（用于排序）。 |

---

## 5) insight（灵感/假设层）

> 强约束：
> 
> - `purpose=responder` 时，默认 `allow_in_responder=false`，且仅 `validated` 项可被提升为事实/策略后出现在 long_term。
> - insight 的存在不意味着必须注入；它是 planner 的候选辅助。

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `insight.usage_policy` | object | Y | `{allow_in_responder:false}` | P/T/R | policy/config | 控制是否允许出现在 responder。 |
| `insight.hypotheses` | array | Y | `[]` | P | insights | 假设列表。 |
| `insight.strategy_sketches` | array | Y | `[]` | P | insights | 策略草图列表。 |
| `insight.patterns` | array | Y | `[]` | P | insights | 模式观察列表。 |

### insight item 通用字段（hypotheses/strategy/pattern）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `*.id` | string | Y | — | P | insights | 主键。 |
| `*.type` | enum | Y | — | P | insights | hypothesis/strategy/pattern。 |
| `*.statement` | string | Y | — | P | insights | 一句话陈述。 |
| `*.trigger` | enum | N | `"synthesis"` | P | insights | conflict/failure/synthesis/analogy。 |
| `*.confidence` | number | N | 0.3 | P | insights | 低置信默认。 |
| `*.validation_state` | enum | Y | `"unvalidated"` | P | insights | unvalidated/testing/validated/rejected。 |
| `*.tests_suggested` | array | N | `[]` | P | insights | 建议验证步骤（问用户/用工具）。 |
| `*.expires_at` | string/enum | N | `"run_end"` | P | insights | TTL（强制）。 |
| `*.sources` | array | N | `[]` | P | insights (+events) | 触发证据引用。 |

---

## 6) citations（统一证据引用）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `citations[]` | object | Y | `[]` | P/T/R | events/tool refs | 引用集合（去重）。 |
| `citations[].id` | string | Y | — | P/T/R | events/tool results | evidence ID。 |
| `citations[].type` | enum | Y | — | P/T/R | events/tool results | message/tool_result/state_patch… |
| `citations[].ts` | string | N | — | P/T/R | events | 时间戳。 |
| `citations[].summary` | string | N | — | P/T/R | composer生成 | 可选短摘要。 |

---

## 7) budget_report（预算与降级）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `budget_report.max_tokens` | int | Y | meta.budget.max_tokens | P/T/R | meta/policy | 总预算。 |
| `budget_report.used_tokens_est` | int | Y | 0 | P/T/R | composer计算 | 估算 token 用量。 |
| `budget_report.section_usage` | object | Y | `{}` | P/T/R | composer计算 | 各分区用量。 |
| `budget_report.degradations` | array | Y | `[]` | P/T/R | composer生成 | 记录降级：raw→quote→summary→ref。 |
| `budget_report.omissions` | array | N | `[]` | P/T/R | composer生成 | 省略项与原因（超预算/低优先级/重复）。 |

---

## 8) explain（可解释选择/过滤/冲突）

| 字段 | 类型 | 必填 | 默认 | 注入模板 | 来源存储 | 说明/约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `explain.selected` | array | Y | `[]` | P/T/R | context_builds | 选入理由（规则命中）。 |
| `explain.omitted` | array | Y | `[]` | P/T/R | context_builds | 省略理由（时间窗/重复/低置信/过期）。 |
| `explain.filters` | object | Y | `{}` | P/T/R | context_builds | 实际应用的 time window、top-k 等。 |
| `explain.conflicts` | array | Y | `[]` | P/T/R | facts | 冲突提示：disputed facts、版本替代等。 |
| `explain.determinism` | object | N | `{}` | P/T/R | composer生成 | 可选：排序键、policy_id，便于复现。 |

---

# 9) 注入模板总览（字段允许性矩阵）

> 说明：这里指“默认允许注入”。字段仍会存在于 packet，但模板可选择不注入到 prompt（例如保留给日志/调试）。
> 

| 分区 | planner | tool | responder |
| --- | --- | --- | --- |
| WM goal/slots/constraints | 强注入 | 强注入 | 中等注入（精简） |
| WM plan/risks | 强注入 | 中等 | 弱（通常不） |
| STM rolling_summary | 中等 | 弱 | 强 |
| STM key_quotes | 中等 | 中等 | 强 |
| conversation_window | 可选 | 可选 | 默认关闭（仅摘要+引用） |
| LTM facts (active+valid) | 强 | 中等 | 强 |
| procedures (task_type) | 强 | 中等 | 弱（通常不） |
| episodes (compressed) | 中等 | 弱 | 中等（少量） |
| insight (unvalidated) | 允许 | 默认不 | 禁止（除非 usage_policy 允许且 validated） |

---

# 10) 字段来源存储映射（逻辑级）

- **wm_state** → `short_term.working_state.*`
- **stm_summary** → `short_term.rolling_summary`, `short_term.key_quotes`
- **events** → `short_term.conversation_window`（可选），`citations`（引用解析）
- **facts + fact_evidence** → `long_term.facts/preferences`, `explain.conflicts`
- **procedures** → `long_term.procedures`
- **episodes (+evidence)** → `long_term.episodes`
- **insights** → `insight.*`
- **context_builds** → `budget_report`, `explain`（持久化回放）
- **candidates**（可选参与解释）→ 用于 `explain.selected/omitted` 的来源说明

---

## 

```jsx
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://yourmem.ai/schemas/memorypacket/v1.json",
  "title": "MemoryPacket",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "meta",
    "short_term",
    "long_term",
    "insight",
    "citations",
    "budget_report",
    "explain"
  ],
  "properties": {
    "meta": {
      "type": "object",
      "additionalProperties": false,
      "required": [
        "schema_version",
        "scope",
        "generated_at",
        "purpose",
        "budget"
      ],
      "properties": {
        "schema_version": {
          "type": "string",
          "const": "v1"
        },
        "scope": {
          "type": "object",
          "additionalProperties": false,
          "required": ["user_id", "agent_id", "session_id", "run_id"],
          "properties": {
            "tenant_id": {
              "type": "string",
              "default": "default"
            },
            "user_id": { "type": "string", "minLength": 1 },
            "agent_id": { "type": "string", "minLength": 1 },
            "session_id": { "type": "string", "minLength": 1 },
            "run_id": { "type": "string", "minLength": 1 }
          }
        },
        "generated_at": {
          "type": "string",
          "format": "date-time"
        },
        "purpose": {
          "type": "string",
          "enum": ["planner", "tool", "responder"],
          "description": "Determines injection policy. Runtime MUST enforce: unvalidated insights are not injected into responder outputs."
        },
        "task_type": {
          "type": "string",
          "default": "generic"
        },
        "cues": {
          "type": "object",
          "additionalProperties": true,
          "default": {},
          "properties": {
            "tags": {
              "type": "array",
              "items": { "type": "string" },
              "default": []
            },
            "entities": {
              "type": "array",
              "items": { "type": "string" },
              "default": []
            },
            "keywords": {
              "type": "array",
              "items": { "type": "string" },
              "default": []
            },
            "time_range": {
              "type": "object",
              "additionalProperties": false,
              "required": [],
              "properties": {
                "start": { "type": "string", "format": "date-time" },
                "end": { "type": "string", "format": "date-time" }
              }
            }
          }
        },
        "budget": {
          "type": "object",
          "additionalProperties": false,
          "required": ["max_tokens", "per_section"],
          "properties": {
            "max_tokens": { "type": "integer", "minimum": 256 },
            "per_section": {
              "type": "object",
              "additionalProperties": false,
              "required": [
                "working_state",
                "facts",
                "procedures",
                "short_term_summary",
                "episodes",
                "insights"
              ],
              "properties": {
                "working_state": { "type": "integer", "minimum": 0 },
                "facts": { "type": "integer", "minimum": 0 },
                "procedures": { "type": "integer", "minimum": 0 },
                "short_term_summary": { "type": "integer", "minimum": 0 },
                "episodes": { "type": "integer", "minimum": 0 },
                "insights": { "type": "integer", "minimum": 0 }
              }
            }
          }
        },
        "policy_id": {
          "type": "string",
          "default": "default"
        }
      }
    },

    "short_term": {
      "type": "object",
      "additionalProperties": false,
      "required": ["working_state", "rolling_summary"],
      "properties": {
        "working_state": {
          "type": "object",
          "additionalProperties": true,
          "required": ["state_version"],
          "properties": {
            "goal": { "type": "string", "default": "" },
            "plan": {
              "type": "array",
              "default": [],
              "items": {
                "type": "object",
                "additionalProperties": false,
                "required": ["step", "status"],
                "properties": {
                  "step": { "type": "string" },
                  "status": { "type": "string", "enum": ["todo", "in_progress", "done"] }
                }
              }
            },
            "slots": { "type": "object", "additionalProperties": true, "default": {} },
            "constraints": { "type": "object", "additionalProperties": true, "default": {} },
            "tool_evidence": {
              "type": "array",
              "default": [],
              "items": {
                "type": "object",
                "additionalProperties": false,
                "required": ["ref", "summary"],
                "properties": {
                  "ref": { "type": "string" },
                  "summary": { "type": "string" }
                }
              }
            },
            "decisions": {
              "type": "array",
              "default": [],
              "items": {
                "type": "object",
                "additionalProperties": true,
                "required": ["statement"],
                "properties": {
                  "statement": { "type": "string" },
                  "evidence_id": { "type": "string" }
                }
              }
            },
            "risks": {
              "type": "array",
              "default": [],
              "items": {
                "type": "object",
                "additionalProperties": true,
                "required": ["risk"],
                "properties": {
                  "risk": { "type": "string" },
                  "mitigation": { "type": "string" }
                }
              }
            },
            "state_version": { "type": "integer", "minimum": 0 }
          }
        },

        "rolling_summary": {
          "type": "string",
          "default": ""
        },

        "key_quotes": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["evidence_id", "quote"],
            "properties": {
              "evidence_id": { "type": "string" },
              "quote": { "type": "string" },
              "role": { "type": "string", "enum": ["user", "assistant", "tool"], "default": "user" },
              "ts": { "type": "string", "format": "date-time" }
            }
          }
        },

        "conversation_window": {
          "type": "array",
          "default": [],
          "description": "Optional recent messages; recommended off by default for responder to preserve budget.",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["evidence_id", "role", "content"],
            "properties": {
              "evidence_id": { "type": "string" },
              "role": { "type": "string", "enum": ["user", "assistant", "tool"] },
              "content": { "type": "string" },
              "ts": { "type": "string", "format": "date-time" }
            }
          }
        },

        "open_loops": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["question", "status"],
            "properties": {
              "question": { "type": "string" },
              "owner": { "type": "string", "enum": ["user", "agent", "system"], "default": "agent" },
              "status": { "type": "string", "enum": ["open", "closed"], "default": "open" },
              "evidence_id": { "type": "string" }
            }
          }
        },

        "last_tool_evidence": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["ref", "summary"],
            "properties": {
              "ref": { "type": "string" },
              "summary": { "type": "string" }
            }
          }
        }
      }
    },

    "long_term": {
      "type": "object",
      "additionalProperties": false,
      "required": ["facts", "procedures", "episodes"],
      "properties": {
        "facts": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["fact_id", "fact_key", "value", "status", "sources"],
            "properties": {
              "fact_id": { "type": "string" },
              "fact_key": { "type": "string", "minLength": 1 },
              "value": {},
              "status": { "type": "string", "enum": ["active", "disputed", "deprecated"], "default": "active" },
              "validity": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "valid_from": { "type": "string", "format": "date-time" },
                  "valid_to": { "type": ["string", "null"], "format": "date-time" }
                }
              },
              "confidence": { "type": "number", "minimum": 0, "maximum": 1, "default": 0.5 },
              "sources": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "scope_level": { "type": "string", "enum": ["user", "agent", "tenant"], "default": "user" },
              "notes": { "type": "string", "default": "" }
            }
          }
        },

        "preferences": {
          "type": "array",
          "default": [],
          "description": "Optional convenience subset of facts; must be consistent with long_term.facts.",
          "items": { "$ref": "#/properties/long_term/properties/facts/items" }
        },

        "procedures": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["procedure_id", "task_type", "content"],
            "properties": {
              "procedure_id": { "type": "string" },
              "task_type": { "type": "string", "minLength": 1 },
              "content": { "type": "object", "additionalProperties": true },
              "priority": { "type": "integer", "default": 0 },
              "sources": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "applicability": {
                "type": "object",
                "additionalProperties": true,
                "default": {}
              }
            }
          }
        },

        "episodes": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["episode_id", "time_range", "summary", "sources"],
            "properties": {
              "episode_id": { "type": "string" },
              "time_range": {
                "type": "object",
                "additionalProperties": false,
                "required": ["start"],
                "properties": {
                  "start": { "type": "string", "format": "date-time" },
                  "end": { "type": "string", "format": "date-time" }
                }
              },
              "summary": { "type": "string" },
              "highlights": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "tags": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "entities": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "sources": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              },
              "compression_level": {
                "type": "string",
                "enum": ["raw", "phase_summary", "milestone"],
                "default": "raw"
              },
              "recency_score": { "type": "number", "minimum": 0, "maximum": 1 }
            }
          }
        }
      }
    },

    "insight": {
      "type": "object",
      "additionalProperties": false,
      "required": ["usage_policy", "hypotheses", "strategy_sketches", "patterns"],
      "properties": {
        "usage_policy": {
          "type": "object",
          "additionalProperties": false,
          "required": ["allow_in_responder"],
          "properties": {
            "allow_in_responder": {
              "type": "boolean",
              "default": false,
              "description": "Runtime policy: if false, injector MUST NOT place unvalidated insight into responder context."
            }
          }
        },
        "hypotheses": {
          "type": "array",
          "default": [],
          "items": { "$ref": "#/$defs/insight_item" }
        },
        "strategy_sketches": {
          "type": "array",
          "default": [],
          "items": { "$ref": "#/$defs/insight_item" }
        },
        "patterns": {
          "type": "array",
          "default": [],
          "items": { "$ref": "#/$defs/insight_item" }
        }
      }
    },

    "citations": {
      "type": "array",
      "default": [],
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["id", "type"],
        "properties": {
          "id": { "type": "string" },
          "type": { "type": "string" },
          "ts": { "type": "string", "format": "date-time" },
          "summary": { "type": "string" }
        }
      }
    },

    "budget_report": {
      "type": "object",
      "additionalProperties": false,
      "required": ["max_tokens", "used_tokens_est", "section_usage", "degradations"],
      "properties": {
        "max_tokens": { "type": "integer", "minimum": 256 },
        "used_tokens_est": { "type": "integer", "minimum": 0 },
        "section_usage": {
          "type": "object",
          "additionalProperties": {
            "type": "integer",
            "minimum": 0
          },
          "default": {}
        },
        "degradations": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["section", "action", "reason"],
            "properties": {
              "section": { "type": "string" },
              "action": { "type": "string" },
              "reason": { "type": "string" }
            }
          }
        },
        "omissions": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["item", "reason"],
            "properties": {
              "item": { "type": "string" },
              "reason": { "type": "string" }
            }
          }
        }
      }
    },

    "explain": {
      "type": "object",
      "additionalProperties": false,
      "required": ["selected", "omitted", "filters", "conflicts"],
      "properties": {
        "selected": {
          "type": "array",
          "default": [],
          "items": { "type": "string" }
        },
        "omitted": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": true,
            "required": ["item", "reason"],
            "properties": {
              "item": { "type": "string" },
              "reason": { "type": "string" }
            }
          }
        },
        "filters": {
          "type": "object",
          "additionalProperties": true,
          "default": {}
        },
        "conflicts": {
          "type": "array",
          "default": [],
          "items": {
            "type": "object",
            "additionalProperties": true,
            "required": ["type", "detail"],
            "properties": {
              "type": { "type": "string" },
              "detail": { "type": "string" },
              "fact_ids": {
                "type": "array",
                "items": { "type": "string" },
                "default": []
              }
            }
          }
        },
        "determinism": {
          "type": "object",
          "additionalProperties": true,
          "default": {},
          "description": "Optional metadata for reproducibility: policy_id, sort_keys, time_window, top_k settings."
        }
      }
    }
  },

  "$defs": {
    "insight_item": {
      "type": "object",
      "additionalProperties": false,
      "required": ["id", "type", "statement", "validation_state"],
      "properties": {
        "id": { "type": "string" },
        "type": { "type": "string", "enum": ["hypothesis", "strategy", "pattern"] },
        "statement": { "type": "string" },
        "trigger": { "type": "string", "enum": ["conflict", "failure", "synthesis", "analogy"], "default": "synthesis" },
        "confidence": { "type": "number", "minimum": 0, "maximum": 1, "default": 0.3 },
        "validation_state": { "type": "string", "enum": ["unvalidated", "testing", "validated", "rejected"], "default": "unvalidated" },
        "tests_suggested": {
          "type": "array",
          "items": { "type": "string" },
          "default": []
        },
        "expires_at": {
          "type": ["string", "null"],
          "default": "run_end",
          "description": "TTL boundary; runtime must enforce expiration/cleanup."
        },
        "sources": {
          "type": "array",
          "items": { "type": "string" },
          "default": []
        }
      }
    }
  }
}

```