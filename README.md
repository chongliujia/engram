# engram
A memory and context engineering system for AI agents, with predictable latency and explainable long-term and short-term memory.

## Quick start (Rust)

```rust
use engram_store::{build_memory_packet, BuildRequest, SqliteStore};
use engram_types::{Purpose, Scope};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = SqliteStore::new("data/engram.db")?;
    let scope = Scope {
        tenant_id: "default".to_string(),
        user_id: "u1".to_string(),
        agent_id: "a1".to_string(),
        session_id: "s1".to_string(),
        run_id: "r1".to_string(),
    };

    let request = BuildRequest::new(scope, Purpose::Planner);
    let packet = build_memory_packet(&store, request)?;
    println!("packet schema: {}", packet.meta.schema_version);
    Ok(())
}
```

## Quick start (Python)

Build and install the extension:

```bash
cd python
maturin develop
```

Use the SDK:

```python
from engram import Memory

mem = Memory(in_memory=True)
scope = {
    "tenant_id": "default",
    "user_id": "u1",
    "agent_id": "a1",
    "session_id": "s1",
    "run_id": "r1",
}
packet = mem.build_memory_packet({"scope": scope, "purpose": "planner"})
print(packet["meta"]["schema_version"])
```

## LangChain / LangGraph adapters (minimal stubs)

- LangChain: `EngramChatMessageHistory`, `EngramContextInjector`
- LangGraph: `EngramCheckpointer`, `EngramNodeMiddleware`

Example (DeepSeek, OpenAI-compatible):
```bash
cp .env.example .env
python examples/langchain_deepseek.py
```

The example reads `DEEPSEEK_API_KEY` from `.env` or your environment.

## Performance testing

Run benchmarks:

```bash
cargo bench -p engram-store
```

Extended and extreme runs:

```bash
ENGRAM_BENCH_EXTENDED=1 cargo bench -p engram-store
ENGRAM_BENCH_EXTREME=1 cargo bench -p engram-store
```

Tuning:

- `ENGRAM_BENCH_INMEMORY_MAX_EVENTS` (default 300000)
- `ENGRAM_BENCH_SQLITE_EVENT_CHUNK` (default 10000)

Generate the HTML summary report:

```bash
python scripts/criterion_report.py
```

Output: `target/criterion/summary.html`

## Sample results (local run)

Your latest benchmark runs show:

- Events scale up to 5M: latency stays ~0.7-2.0ms (SQLite) with stable recall.
- Candidate scale up to 5k: ~2.8ms (InMemory) / ~10.3ms (SQLite).

These results support the core guarantee: total memory can grow while recall stays fast
because candidate sets remain small and bounded.
