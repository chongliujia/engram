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

Enable MySQL/Postgres backends (rebuild required):

```bash
cd python
maturin develop --features mysql,postgres
```

Python backends:

```python
Memory(backend="mysql", dsn="mysql://user:pass@localhost:3306/engram")
Memory(backend="postgres", dsn="postgres://user:pass@localhost:5432/engram")
Memory(backend="mysql", dsn="mysql://user:pass@localhost:3306", database="engram")
```

## Advanced Usage

### Asynchronous Support

Use `AsyncMemory` for non-blocking I/O in async applications (e.g., FastAPI, multiple agents).

```python
import asyncio
from engram import AsyncMemory

async def main():
    mem = AsyncMemory(in_memory=True)
    # Concurrent writes
    await asyncio.gather(
        mem.append_event({...}),
        mem.append_event({...})
    )
    # Async read
    packet = await mem.build_memory_packet({...})

asyncio.run(main())
```

See [examples/async_demo.py](examples/async_demo.py) for a complete example.

### Observability & Tracing

Engram emits structured logs (via Rust `tracing` mapped to Python `logging`) to help you understand memory retrieval decisions.

```python
import logging
logging.basicConfig(level=logging.INFO)
# Enable debug logs for engram to see candidate counts and budget trimming
logging.getLogger("engram_store").setLevel(logging.DEBUG)
```

See [examples/observability_demo.py](examples/observability_demo.py).

### Memory Policies & Budgeting

Control the size and content of the `MemoryPacket` using strict budgets and policies.

```python
strict_policy = {
    "max_facts": 5,
    "max_total_candidates": 20
}
budget = {
    "max_tokens": 1000,
    "per_section": {"facts": 200}
}
packet = mem.build_memory_packet({
    "scope": ..., 
    "purpose": "planner",
    "policy": strict_policy, 
    "budget": budget
})
```

See [examples/policy_demo.py](examples/policy_demo.py).

## Storage backends

- SQLite (default, no feature flag)
- Postgres (`--features postgres`)
- MySQL (`--features mysql`)

Example (MySQL):
```rust
use engram_store::MySqlStore;

let store = MySqlStore::new("mysql://user:pass@localhost:3306/engram")?;
```

Notes:
- If the database does not exist, Postgres/MySQL will create it on first connect.
- If the DSN omits a database name, it defaults to `engram`.

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

Include MySQL/Postgres:

```bash
cargo bench -p engram-store --features mysql,postgres
```

For large SQLite datasets, set `ENGRAM_BENCH_SQLITE_MODE=file` (or `auto`) to use a file-backed DB and avoid memory pressure.

Tuning:

- `ENGRAM_BENCH_INMEMORY_MAX_EVENTS` (default 300000)
- `ENGRAM_BENCH_SQLITE_MAX_EVENTS` (default 1000000, in-memory SQLite cap)
- `ENGRAM_BENCH_SQLITE_MODE` (`memory`, `file`, or `auto`, auto switches to file when events exceed `ENGRAM_BENCH_SQLITE_MAX_EVENTS`)
- `ENGRAM_BENCH_SQLITE_DIR` (directory for file-mode SQLite DBs)
- `ENGRAM_BENCH_SQLITE_FILE` (optional fixed SQLite file path)
- `ENGRAM_BENCH_SQLITE_EVENT_CHUNK` (default 10000)
- `ENGRAM_BENCH_MYSQL_DSN` / `ENGRAM_BENCH_POSTGRES_DSN`
- `ENGRAM_BENCH_MYSQL_MAX_EVENTS` / `ENGRAM_BENCH_POSTGRES_MAX_EVENTS`
- `ENGRAM_BENCH_RESET_DB` (set to `1` to truncate SQL tables before each dataset)
- `ENGRAM_BENCH_EVENTS_SCALES` (comma-separated, extra event sizes for event-scale group)
- `ENGRAM_BENCH_WRITE_EVENTS_SCALES` (comma-separated, extra event sizes for append benchmarks)
- `ENGRAM_BENCH_WRITE_FACT_SCALES` (comma-separated, extra fact sizes for upsert benchmarks)
- `ENGRAM_BENCH_BULK_EVENTS_SCALES` (comma-separated, extra event sizes for bulk append)
- `ENGRAM_BENCH_BULK_EVENT_BATCH` (default 500)

Unified bench config file:

- Copy `bench/engram_bench.env.example` to `bench/engram_bench.env` and edit once.
- Set `ENGRAM_BENCH_CONFIG=/path/to/engram_bench.env` to override the default path.

Benchmark groups include:

- `build_memory_packet_events_scale`
- `build_memory_packet_candidate_scale`
- `store_ops_list_events`
- `store_ops_list_facts`
- `store_ops_list_episodes`
- `store_ops_list_insights`
- `store_ops_list_procedures`
- `store_ops_append_event`
- `store_ops_append_events_bulk`
- `store_ops_upsert_fact`

When you run with `--features mysql,postgres` and provide DSNs, the same groups are recorded for MySQL/Postgres alongside SQLite and in-memory.

Generate the HTML summary report:

```bash
python scripts/criterion_report.py
```

Output: `target/criterion/summary.html`

If `target/python_bench.json`, `target/python_load.json`, or `target/python_soak.json` exist, they are included in the summary.

Python benchmarks (optional):

```bash
cd python
maturin develop --features mysql,postgres
ENGRAM_BENCH_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_BENCH_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/bench_backends.py
```

Outputs: `target/python_bench.json`, `target/python_bench.html`, `target/python_bench_prev.json`

Python load test (optional):

```bash
cd python
maturin develop --features mysql,postgres
ENGRAM_LOAD_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_LOAD_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/load_test.py --duration 60 --concurrency 8
```

Outputs: `target/python_load.json`, `target/python_load_prev.json`

Python soak test (optional):

```bash
cd python
maturin develop --features mysql,postgres
ENGRAM_SOAK_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_SOAK_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/soak_test.py --duration 600 --interval 60
```

Outputs: `target/python_soak.json`, `target/python_soak_prev.json`

## Sample results (local run)

Your latest benchmark runs show:

- Events scale up to 5M: latency stays ~0.7-2.0ms (SQLite) with stable recall.
- Candidate scale up to 5k: ~2.8ms (InMemory) / ~10.3ms (SQLite).

These results support the core guarantee: total memory can grow while recall stays fast
because candidate sets remain small and bounded.
