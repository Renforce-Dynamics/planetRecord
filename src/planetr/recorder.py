"""Legacy stream names over the generic SessionWriter; new clients register streams explicitly."""

from .session import SessionWriter


class PlanetRRecorder(SessionWriter):
    def __init__(self, directory, flush_interval_s=0.5, *, streams=None):
        super().__init__(
            directory,
            streams
            or {
                "source": {"schema": "planetu.nokov.v1", "filename": "nokov.jsonl"},
                "trace": {"schema": "planetu.trace.v1", "filename": "trace.jsonl"},
                "onboard": {
                    "schema": "agi3.onboard_debug.v1",
                    "filename": "onboard.jsonl",
                },
                "sim2sim": {
                    "schema": "agi3.onboard_debug.v1",
                    "filename": "sim2sim.jsonl",
                },
            },
            flush_interval_s=flush_interval_s,
        )

    def record(self, kind, payload, metadata=None):
        if kind == "trace":
            payload = {**payload, "record_type": "trace"}
        return super().record(kind, payload, metadata)
