"""Versioned recording envelopes, independent of producer and storage services."""

from dataclasses import dataclass, asdict
import json
import math
import time


@dataclass(frozen=True)
class RecordEnvelope:
    session_id: str
    stream: str
    schema: str
    producer: str
    sequence: int
    captured_ns: int
    payload: dict
    clock_domain: str = "monotonic"
    version: int = 1

    def __post_init__(self):
        if self.version != 1 or not all(
            isinstance(x, str) and x
            for x in (
                self.session_id,
                self.stream,
                self.schema,
                self.producer,
                self.clock_domain,
            )
        ):
            raise ValueError("invalid recording identity/schema")
        if (
            not isinstance(self.sequence, int)
            or self.sequence < 0
            or not isinstance(self.captured_ns, int)
            or self.captured_ns < 0
        ):
            raise ValueError("invalid sequence/timestamp")
        if not isinstance(self.payload, dict):
            raise ValueError("record payload must be a mapping")

    def encode(self):
        return json.dumps(asdict(self), separators=(",", ":"), allow_nan=False).encode()

    @classmethod
    def decode(cls, data):
        if len(data) > 65507:
            raise ValueError("record exceeds datagram limit")
        return cls(**json.loads(data))


__all__ = ["RecordEnvelope"]
