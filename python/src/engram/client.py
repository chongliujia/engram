import json

from ._core import EngramStore


class Memory:
    def __init__(self, path="data/engram.db", in_memory=False):
        if in_memory:
            self._store = EngramStore.in_memory()
        else:
            self._store = EngramStore(path)

    def append_event(self, event):
        self._store.append_event(json.dumps(event))

    def list_events(self, scope, time_range=None, limit=None):
        payload = json.dumps(time_range) if time_range is not None else None
        return json.loads(self._store.list_events(json.dumps(scope), payload, limit))

    def get_working_state(self, scope):
        data = self._store.get_working_state(json.dumps(scope))
        return json.loads(data) if data is not None else None

    def patch_working_state(self, scope, patch):
        return json.loads(
            self._store.patch_working_state(json.dumps(scope), json.dumps(patch))
        )

    def get_stm(self, scope):
        data = self._store.get_stm(json.dumps(scope))
        return json.loads(data) if data is not None else None

    def update_stm(self, scope, stm_state):
        self._store.update_stm(json.dumps(scope), json.dumps(stm_state))

    def list_facts(self, scope, fact_filter=None):
        payload = json.dumps(fact_filter) if fact_filter is not None else None
        return json.loads(self._store.list_facts(json.dumps(scope), payload))

    def upsert_fact(self, scope, fact):
        self._store.upsert_fact(json.dumps(scope), json.dumps(fact))

    def list_episodes(self, scope, episode_filter=None):
        payload = json.dumps(episode_filter) if episode_filter is not None else None
        return json.loads(self._store.list_episodes(json.dumps(scope), payload))

    def append_episode(self, scope, episode):
        self._store.append_episode(json.dumps(scope), json.dumps(episode))

    def list_procedures(self, scope, task_type, limit=None):
        return json.loads(
            self._store.list_procedures(json.dumps(scope), task_type, limit)
        )

    def upsert_procedure(self, scope, procedure):
        self._store.upsert_procedure(json.dumps(scope), json.dumps(procedure))

    def list_insights(self, scope, insight_filter=None):
        payload = json.dumps(insight_filter) if insight_filter is not None else None
        return json.loads(self._store.list_insights(json.dumps(scope), payload))

    def append_insight(self, scope, insight):
        self._store.append_insight(json.dumps(scope), json.dumps(insight))

    def write_context_build(self, scope, packet):
        self._store.write_context_build(json.dumps(scope), json.dumps(packet))

    def list_context_builds(self, scope, limit=None):
        return json.loads(self._store.list_context_builds(json.dumps(scope), limit))

    def build_memory_packet(self, request):
        return json.loads(self._store.build_memory_packet(json.dumps(request)))
