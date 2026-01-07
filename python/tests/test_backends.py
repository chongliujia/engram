import os
import tempfile
import time
import unittest
import uuid

from engram import Memory


def unique_suffix():
    return uuid.uuid4().hex


def sample_scope():
    suffix = unique_suffix()
    return {
        "tenant_id": f"tenant-{suffix}",
        "user_id": f"user-{suffix}",
        "agent_id": f"agent-{suffix}",
        "session_id": f"session-{suffix}",
        "run_id": f"run-{suffix}",
    }


def sample_event(scope, event_id):
    return {
        "event_id": event_id,
        "scope": scope,
        "ts_ms": int(time.time() * 1000),
        "kind": "message",
        "payload": {"role": "user", "content": "hello"},
        "tags": ["intro"],
        "entities": [],
    }


def roundtrip_memory(mem, test_case):
    scope = sample_scope()
    event_id = f"e-{unique_suffix()}"
    mem.append_event(sample_event(scope, event_id))

    events = mem.list_events(scope)
    test_case.assertEqual(len(events), 1)
    test_case.assertEqual(events[0]["event_id"], event_id)

    packet = mem.build_memory_packet(
        {"scope": scope, "purpose": "planner", "task_type": "generic"}
    )
    test_case.assertEqual(packet["meta"]["scope"]["run_id"], scope["run_id"])


class BackendSmokeTests(unittest.TestCase):
    def test_sqlite_in_memory(self):
        mem = Memory(in_memory=True)
        roundtrip_memory(mem, self)

    def test_sqlite_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "engram.db")
            mem = Memory(path=path)
            roundtrip_memory(mem, self)

    def test_mysql(self):
        dsn = os.getenv("ENGRAM_TEST_MYSQL_DSN")
        if not dsn:
            self.skipTest("ENGRAM_TEST_MYSQL_DSN not set")
        database = os.getenv("ENGRAM_TEST_MYSQL_DB")
        mem = Memory(backend="mysql", dsn=dsn, database=database)
        roundtrip_memory(mem, self)

    def test_postgres(self):
        dsn = os.getenv("ENGRAM_TEST_POSTGRES_DSN")
        if not dsn:
            self.skipTest("ENGRAM_TEST_POSTGRES_DSN not set")
        database = os.getenv("ENGRAM_TEST_POSTGRES_DB")
        mem = Memory(backend="postgres", dsn=dsn, database=database)
        roundtrip_memory(mem, self)


if __name__ == "__main__":
    unittest.main()
