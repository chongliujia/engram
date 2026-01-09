# Engram Python SDK

This package provides a thin Python wrapper around the Engram Rust core.

## Quick start

```python
from engram import Memory

mem = Memory(path="data/engram.db")
scope = {
    "tenant_id": "default",
    "user_id": "u1",
    "agent_id": "a1",
    "session_id": "s1",
    "run_id": "r1",
}

mem.append_event(
    {
        "event_id": "e1",
        "scope": scope,
        "ts": "2024-01-01T00:00:00Z",
        "kind": "message",
        "payload": {"role": "user", "content": "hello"},
        "tags": ["intro"],
        "entities": [],
    }
)

packet = mem.build_memory_packet(
    {"scope": scope, "purpose": "planner", "task_type": "generic"}
)
```

## Install (with database backends)

SQLite-only (default):

```bash
maturin develop
```

Enable MySQL/Postgres (rebuild required):

```bash
maturin develop --features mysql,postgres
```

## Backends

SQLite (default):

```python
mem = Memory()
```

MySQL / Postgres (compile with features first):

```bash
maturin develop --features mysql,postgres
```

```python
mem = Memory(
    backend="mysql",
    dsn="mysql://user:pass@localhost:3306/engram",
)

mem = Memory(
    backend="postgres",
    dsn="postgres://user:pass@localhost:5432/engram",
)

# Use a separate database name
mem = Memory(
    backend="mysql",
    dsn="mysql://user:pass@localhost:3306",
    database="engram",
)
```

Notes:
- If the DSN omits a database name, it defaults to `engram`.
- If the database does not exist, it will be created on first connect.

## Tests

```bash
maturin develop --features mysql,postgres
ENGRAM_TEST_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_TEST_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python -m unittest python/tests/test_backends.py
```

## Benchmarks (Python)

```bash
maturin develop --features mysql,postgres
ENGRAM_BENCH_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_BENCH_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/bench_backends.py
```

Outputs:
- `target/python_bench.json`
- `target/python_bench.html`
- `target/python_bench_prev.json` (auto-saved previous run)

Config file (optional):

- Copy `bench/engram_bench.env.example` to `bench/engram_bench.env`.
- Set `ENGRAM_BENCH_CONFIG=/path/to/engram_bench.env` to use a custom path.
- The Python benchmark scripts read this file for defaults if present.

## Load test (Python)

```bash
maturin develop --features mysql,postgres
ENGRAM_LOAD_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_LOAD_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/load_test.py --duration 60 --concurrency 8
```

Outputs:
- `target/python_load.json`
- `target/python_load_prev.json` (auto-saved previous run)

## Soak test (Python)

```bash
maturin develop --features mysql,postgres
ENGRAM_SOAK_MYSQL_DSN="mysql://user:pass@localhost:3306/engram" \
ENGRAM_SOAK_POSTGRES_DSN="postgres://user:pass@localhost:5432/engram" \
python python/scripts/soak_test.py --duration 600 --interval 60
```

Outputs:
- `target/python_soak.json`
- `target/python_soak_prev.json` (auto-saved previous run)
