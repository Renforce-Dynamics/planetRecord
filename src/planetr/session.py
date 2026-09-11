"""Task-neutral stream storage with explicit schemas and integrity metadata."""

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
import json
import os
import re
import time


@dataclass(frozen=True)
class StreamSpec:
    schema: str
    filename: str


class SessionWriter:
    def __init__(
        self,
        directory,
        streams,
        *,
        session_id=None,
        metadata=None,
        flush_interval_s=0.5,
    ):
        if flush_interval_s <= 0:
            raise ValueError("flush interval must be positive")
        self.session_id = session_id or datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.session_id) or self.session_id in {
            ".",
            "..",
        }:
            raise ValueError("invalid session directory name")
        self.session_dir = Path(directory).expanduser().resolve() / self.session_id
        self.streams = {
            k: v if isinstance(v, StreamSpec) else StreamSpec(**v)
            for k, v in streams.items()
        }
        if not self.streams:
            raise ValueError("at least one stream must be registered")
        for spec in self.streams.values():
            p = Path(spec.filename)
            if (
                not spec.schema
                or p.is_absolute()
                or ".." in p.parts
                or p.suffix != ".jsonl"
            ):
                raise ValueError(
                    "stream must have schema and a relative .jsonl filename"
                )
        if len({v.filename for v in self.streams.values()}) != len(self.streams):
            raise ValueError("stream filenames must be unique")
        self.session_dir.mkdir(parents=True, exist_ok=False)
        self.files = {}
        self.counts = {k: 0 for k in self.streams}
        self.closed = False
        self.flush_interval_s = flush_interval_s
        self.last_flush = time.monotonic()
        self.metadata = {
            "schema": "planetr.session.v2",
            "session_id": self.session_id,
            "complete": False,
            "streams": {k: vars(v) for k, v in self.streams.items()},
            "metadata": metadata or {},
        }
        self._meta()

    def _meta(self):
        p = self.session_dir / "meta.json.tmp"
        p.write_text(json.dumps(self.metadata, indent=2, allow_nan=False))
        p.replace(self.session_dir / "meta.json")

    def record(self, kind, payload, metadata=None):
        if self.closed:
            raise RuntimeError("session is closed")
        if kind not in self.streams:
            raise ValueError(f"unregistered stream: {kind}")
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False)
        if kind not in self.files:
            spec = self.streams[kind]
            p = self.session_dir / spec.filename
            p.parent.mkdir(parents=True, exist_ok=True)
            f = p.open("w")
            f.write(json.dumps({"__header__": True, "schema": spec.schema}) + "\n")
            self.files[kind] = f
        self.files[kind].write(encoded + "\n")
        self.counts[kind] += 1
        if metadata:
            self.metadata["metadata"].update(metadata)
        if time.monotonic() - self.last_flush >= self.flush_interval_s:
            self.flush()

    def write(self, envelope):
        from dataclasses import asdict

        if envelope.session_id != self.session_id:
            raise ValueError("session ID mismatch")
        if (
            envelope.stream not in self.streams
            or self.streams[envelope.stream].schema != envelope.schema
        ):
            raise ValueError("stream/schema mismatch")
        self.record(envelope.stream, asdict(envelope))

    def flush(self):
        for f in self.files.values():
            f.flush()
        self.last_flush = time.monotonic()

    def close(self, stats=None):
        if self.closed:
            return
        self.flush()
        for f in self.files.values():
            f.close()
        self.closed = True
        stats = stats or {}
        self.metadata.update(
            counts=self.counts,
            stats=stats,
            complete=not any(stats.get(k, 0) for k in ("dropped", "errors", "gaps")),
        )
        self._meta()
        (self.session_dir / "recording_stats.json").write_text(
            json.dumps({"counts": self.counts, **stats}, indent=2)
        )


def replay(session_dir, *, speed=0.0, streams=None):
    """Yield recorded envelopes. Cross-clock records are never assigned a synthetic ordering."""
    if speed < 0:
        raise ValueError("replay speed must be nonnegative")
    root = Path(session_dir)
    meta = json.loads((root / "meta.json").read_text())
    rows = []
    for name, spec in meta["streams"].items():
        if streams and name not in streams:
            continue
        p = root / spec["filename"]
        if not p.exists():
            continue
        for line in p.read_text().splitlines()[1:]:
            rows.append(json.loads(line))
    domains = {(r.get("producer"), r.get("clock_domain")) for r in rows}
    if speed > 0 and len(domains) > 1:
        raise ValueError("timed replay needs one producer clock domain")
    if len(domains) <= 1:
        rows.sort(key=lambda r: (r.get("captured_ns", 0), r.get("sequence", 0)))
    previous = None
    for row in rows:
        now = row.get("captured_ns", 0)
        if speed > 0 and previous is not None:
            time.sleep(max(0, (now - previous) / 1e9 / speed))
        previous = now
        yield row
