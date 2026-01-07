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
