import uuid

from ..client import Memory


class EngramCheckpointer:
    def __init__(self, memory: Memory, scope: dict):
        self._memory = memory
        self._scope = scope

    def get(self):
        return self._memory.get_working_state(self._scope)

    def put(self, state: dict):
        return self._memory.patch_working_state(self._scope, state)


class EngramNodeMiddleware:
    def __init__(self, memory: Memory, scope: dict):
        self._memory = memory
        self._scope = scope

    def before_node(self, purpose: str = "planner", task_type: str | None = None):
        request = {"scope": self._scope, "purpose": purpose}
        if task_type is not None:
            request["task_type"] = task_type
        return self._memory.build_memory_packet(request)

    def after_node(self, events: list[dict]):
        for event in events:
            if "event_id" not in event:
                event["event_id"] = str(uuid.uuid4())
            if "scope" not in event:
                event["scope"] = self._scope
            self._memory.append_event(event)
