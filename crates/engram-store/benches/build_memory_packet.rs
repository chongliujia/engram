use std::env;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion};
use engram_store::{
    build_memory_packet, BuildRequest, Event, EventKind, InMemoryStore, RecallCues, RecallPolicy,
    SqliteStore, Store, StmState, WorkingStatePatch,
};
use engram_types::{
    CompressionLevel, Episode, Fact, FactStatus, InsightItem, InsightTrigger, InsightType, JsonMap,
    Procedure, Scope, ScopeLevel, TimeRange, Validity,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;

const TENANT_ID: &str = "default";
const USER_ID: &str = "bench-user";
const AGENT_ID: &str = "bench-agent";
const SESSION_ID: &str = "bench-session";
const RUN_ID: &str = "bench-run";
const MAX_IN_MEMORY_EVENTS: usize = 300_000;
const SQLITE_EVENT_CHUNK: usize = 10_000;

#[derive(Clone, Copy, Debug)]
struct DatasetSize {
    events: usize,
    facts: usize,
    episodes: usize,
    procedures: usize,
    insights: usize,
}

impl DatasetSize {
    fn label(&self) -> String {
        format!(
            "events{}_facts{}_episodes{}_procedures{}_insights{}",
            self.events, self.facts, self.episodes, self.procedures, self.insights
        )
    }
}

fn scope() -> Scope {
    Scope {
        tenant_id: TENANT_ID.to_string(),
        user_id: USER_ID.to_string(),
        agent_id: AGENT_ID.to_string(),
        session_id: SESSION_ID.to_string(),
        run_id: RUN_ID.to_string(),
    }
}

fn main_request() -> BuildRequest {
    let mut request = BuildRequest::new(scope(), engram_types::Purpose::Planner);
    request.task_type = Some("summary".to_string());
    request.cues = RecallCues {
        tags: vec!["alpha".to_string()],
        entities: vec!["entity1".to_string()],
        keywords: vec!["engram".to_string()],
        time_range: None,
    };
    request.policy = RecallPolicy {
        max_total_candidates: 100,
        max_facts: 30,
        max_procedures: 5,
        max_episodes: 20,
        max_insights: 10,
        max_key_quotes: 10,
        conversation_window: 5,
        episode_time_window_days: 30,
        last_tool_evidence_limit: 3,
        include_conversation_window: false,
        include_insights_in_tool: false,
        allow_insights_in_responder: false,
    };
    request.persist = false;
    request
}

fn seed_store_common<S: Store>(store: &S, size: DatasetSize) {
    let mut rng = StdRng::seed_from_u64(42);
    let scope = scope();
    let now = Utc::now();

    store
        .patch_working_state(
            &scope,
            WorkingStatePatch {
                goal: Some("benchmark".to_string()),
                plan: Some(vec!["step1".to_string(), "step2".to_string()]),
                ..WorkingStatePatch::default()
            },
        )
        .unwrap();

    store
        .update_stm(
            &scope,
            StmState {
                rolling_summary: "rolling summary".to_string(),
                key_quotes: vec![],
            },
        )
        .unwrap();

    for idx in 0..size.facts {
        let fact = Fact {
            fact_id: format!("f{}", idx),
            fact_key: format!("pref.key.{}", idx),
            value: json!({ "value": idx }),
            status: FactStatus::Active,
            validity: Validity::default(),
            confidence: 0.5 + (idx as f64 / size.facts.max(1) as f64) * 0.5,
            sources: vec!["e0".to_string()],
            scope_level: ScopeLevel::User,
            notes: String::new(),
        };
        store.upsert_fact(&scope, fact).unwrap();
    }

    for idx in 0..size.episodes {
        let episode = Episode {
            episode_id: format!("ep{}", idx),
            time_range: TimeRange {
                start: now - ChronoDuration::days(idx as i64 % 30),
                end: None,
            },
            summary: format!("episode {}", idx),
            highlights: vec![format!("highlight {}", idx)],
            tags: vec![if idx % 2 == 0 { "alpha" } else { "beta" }.to_string()],
            entities: vec!["entity1".to_string()],
            sources: vec!["e0".to_string()],
            compression_level: CompressionLevel::Raw,
            recency_score: None,
        };
        store.append_episode(&scope, episode).unwrap();
    }

    for idx in 0..size.procedures {
        let procedure = Procedure {
            procedure_id: format!("p{}", idx),
            task_type: "summary".to_string(),
            content: json!({ "step": idx }),
            priority: (size.procedures as i32) - idx as i32,
            sources: vec![],
            applicability: JsonMap::new(),
        };
        store.upsert_procedure(&scope, procedure).unwrap();
    }

    for idx in 0..size.insights {
        let insight = InsightItem {
            id: format!("i{}", idx),
            kind: if idx % 2 == 0 {
                InsightType::Hypothesis
            } else {
                InsightType::Pattern
            },
            statement: format!("insight {}", idx),
            trigger: if idx % 2 == 0 {
                InsightTrigger::Synthesis
            } else {
                InsightTrigger::Analogy
            },
            confidence: rng.gen_range(0.1..0.9),
            validation_state: engram_types::ValidationState::Testing,
            tests_suggested: vec![],
            expires_at: "run_end".to_string(),
            sources: vec![],
        };
        store.append_insight(&scope, insight).unwrap();
    }
}

fn seed_events_in_memory(store: &InMemoryStore, size: DatasetSize) {
    let scope = scope();
    let now = Utc::now();
    for idx in 0..size.events {
        let event = Event {
            event_id: format!("e{}", idx),
            scope: scope.clone(),
            ts: now - ChronoDuration::seconds(idx as i64),
            kind: EventKind::Message,
            payload: json!({ "role": "user", "content": format!("message {}", idx) }),
            tags: vec![if idx % 2 == 0 { "alpha" } else { "beta" }.to_string()],
            entities: vec!["entity1".to_string()],
        };
        store.append_event(event).unwrap();
    }
}

fn seed_events_sqlite(store: &SqliteStore, size: DatasetSize) {
    let scope = scope();
    let now = Utc::now();
    let chunk_size = sqlite_event_chunk();
    let mut buffer = Vec::with_capacity(chunk_size);
    for idx in 0..size.events {
        buffer.push(Event {
            event_id: format!("e{}", idx),
            scope: scope.clone(),
            ts: now - ChronoDuration::seconds(idx as i64),
            kind: EventKind::Message,
            payload: json!({ "role": "user", "content": format!("message {}", idx) }),
            tags: vec![if idx % 2 == 0 { "alpha" } else { "beta" }.to_string()],
            entities: vec!["entity1".to_string()],
        });
        if buffer.len() >= chunk_size {
            store.append_events_bulk(&buffer).unwrap();
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        store.append_events_bulk(&buffer).unwrap();
    }
}

fn bench_in_memory(group: &mut BenchmarkGroup<'_, WallTime>, size: DatasetSize) {
    if size.events > max_in_memory_events() {
        return;
    }
    let store = InMemoryStore::new();
    seed_store_common(&store, size);
    seed_events_in_memory(&store, size);
    let request = main_request();

    group.bench_with_input(
        BenchmarkId::new("memory", size.label()),
        &size,
        |b, _| {
            b.iter(|| {
                let _ = build_memory_packet(&store, request.clone()).unwrap();
            })
        },
    );
}

fn bench_sqlite(group: &mut BenchmarkGroup<'_, WallTime>, size: DatasetSize) {
    let store = SqliteStore::new_in_memory().unwrap();
    seed_store_common(&store, size);
    seed_events_sqlite(&store, size);
    let request = main_request();

    group.bench_with_input(
        BenchmarkId::new("sqlite", size.label()),
        &size,
        |b, _| {
            b.iter(|| {
                let _ = build_memory_packet(&store, request.clone()).unwrap();
            })
        },
    );
}

fn build_memory_packet_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_memory_packet_events_scale");
    group.sample_size(25);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(6));

    let mut event_scale_sizes = vec![
        DatasetSize {
            events: 200,
            facts: 50,
            episodes: 40,
            procedures: 10,
            insights: 20,
        },
        DatasetSize {
            events: 2000,
            facts: 200,
            episodes: 200,
            procedures: 30,
            insights: 50,
        },
        DatasetSize {
            events: 10000,
            facts: 500,
            episodes: 1000,
            procedures: 50,
            insights: 100,
        },
        DatasetSize {
            events: 100000,
            facts: 500,
            episodes: 1000,
            procedures: 50,
            insights: 100,
        },
        DatasetSize {
            events: 300000,
            facts: 500,
            episodes: 1000,
            procedures: 50,
            insights: 100,
        },
    ];

    if is_extended() {
        event_scale_sizes.push(DatasetSize {
            events: 1_000_000,
            facts: 500,
            episodes: 1000,
            procedures: 50,
            insights: 100,
        });
    }

    if is_extreme() {
        event_scale_sizes.push(DatasetSize {
            events: 5_000_000,
            facts: 500,
            episodes: 1000,
            procedures: 50,
            insights: 100,
        });
    }

    for size in event_scale_sizes {
        bench_in_memory(&mut group, size);
        bench_sqlite(&mut group, size);
    }
    group.finish();

    let mut candidate_group = c.benchmark_group("build_memory_packet_candidate_scale");
    candidate_group.sample_size(25);
    candidate_group.warm_up_time(Duration::from_secs(3));
    candidate_group.measurement_time(Duration::from_secs(6));

    let mut candidate_scale_sizes = vec![
        DatasetSize {
            events: 2000,
            facts: 50,
            episodes: 40,
            procedures: 10,
            insights: 20,
        },
        DatasetSize {
            events: 2000,
            facts: 200,
            episodes: 200,
            procedures: 30,
            insights: 50,
        },
        DatasetSize {
            events: 2000,
            facts: 1000,
            episodes: 1000,
            procedures: 80,
            insights: 200,
        },
        DatasetSize {
            events: 2000,
            facts: 2000,
            episodes: 2000,
            procedures: 120,
            insights: 300,
        },
    ];

    if is_extended() {
        candidate_scale_sizes.push(DatasetSize {
            events: 2000,
            facts: 5000,
            episodes: 5000,
            procedures: 200,
            insights: 500,
        });
    }

    if is_extreme() {
        candidate_scale_sizes.push(DatasetSize {
            events: 2000,
            facts: 10_000,
            episodes: 10_000,
            procedures: 300,
            insights: 800,
        });
    }

    for size in candidate_scale_sizes {
        bench_in_memory(&mut candidate_group, size);
        bench_sqlite(&mut candidate_group, size);
    }
    candidate_group.finish();
}

fn is_extended() -> bool {
    matches!(
        env::var("ENGRAM_BENCH_EXTENDED").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn is_extreme() -> bool {
    matches!(
        env::var("ENGRAM_BENCH_EXTREME").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn max_in_memory_events() -> usize {
    if let Ok(value) = env::var("ENGRAM_BENCH_INMEMORY_MAX_EVENTS") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed;
        }
    }
    MAX_IN_MEMORY_EVENTS
}

fn sqlite_event_chunk() -> usize {
    if let Ok(value) = env::var("ENGRAM_BENCH_SQLITE_EVENT_CHUNK") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed.max(1);
        }
    }
    SQLITE_EVENT_CHUNK
}

criterion_group!(benches, build_memory_packet_bench);
criterion_main!(benches);
