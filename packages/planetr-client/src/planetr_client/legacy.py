"""Non-blocking, full-rate acquisition and planner trace recording."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import threading
import time
from typing import Any, Mapping
import zlib


@dataclass(frozen=True, slots=True)
class RecordingStats:
    session_dir: str
    source_frames_enqueued: int
    trace_frames_enqueued: int
    records_written: int
    records_dropped: int
    queue_depth: int
    error: str


class SessionRecorder:
    """Write replayable normalized mocap frames off the hot path."""

    def __init__(
        self,
        directory: str | Path,
        *,
        robot: str,
        source_kind: str = "nokov",
        source_serializer=lambda value: value,
        queue_capacity: int = 16_384,
        flush_interval_s: float = 0.5,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self.session_dir = Path(directory).expanduser().resolve() / (f"{stamp}_{robot}")
        self.session_dir.mkdir(parents=True, exist_ok=False)
        self.queue_capacity = int(queue_capacity)
        self.flush_interval_s = float(flush_interval_s)
        self.source_kind = str(source_kind)
        self.source_serializer = source_serializer
        self._condition = threading.Condition()
        self._queue: deque[tuple[str, object]] = deque()
        self._running = False
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._source_frames_enqueued = 0
        self._trace_frames_enqueued = 0
        self._records_written = 0
        self._records_dropped = 0
        meta = {
            "schema": "planetu.session.v1",
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "robot": str(robot),
            "source_kind": self.source_kind,
            **dict(metadata or {}),
        }
        (self.session_dir / "meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @property
    def error(self) -> BaseException | None:
        return self._error

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            name="planet_pingpong-recorder",
            daemon=True,
        )
        self._thread.start()

    def record_source(self, snapshot) -> None:
        self._enqueue("source", snapshot)
        self._source_frames_enqueued += 1

    def record_trace(self, trace: object) -> None:
        self._enqueue("trace", trace)
        self._trace_frames_enqueued += 1

    def _enqueue(self, kind: str, value: object) -> None:
        if not self._running:
            return
        with self._condition:
            if len(self._queue) >= self.queue_capacity:
                self._queue.popleft()
                self._records_dropped += 1
            self._queue.append((kind, value))
            self._condition.notify()

    def stats(self) -> RecordingStats:
        with self._condition:
            depth = len(self._queue)
        return RecordingStats(
            session_dir=str(self.session_dir),
            source_frames_enqueued=self._source_frames_enqueued,
            trace_frames_enqueued=self._trace_frames_enqueued,
            records_written=self._records_written,
            records_dropped=self._records_dropped,
            queue_depth=depth,
            error="" if self._error is None else str(self._error),
        )

    def close(self, timeout_s: float = 30.0) -> None:
        self._running = False
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=max(float(timeout_s), 0.0))
            if self._thread.is_alive():
                self._error = TimeoutError(
                    "recording queue did not flush before shutdown timeout"
                )
            else:
                self._thread = None
        (self.session_dir / "recording_stats.json").write_text(
            json.dumps(asdict(self.stats()), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _run(self) -> None:
        source_path = self.session_dir / f"{self.source_kind}.jsonl"
        trace_path = self.session_dir / "trace.jsonl"
        try:
            with (
                source_path.open("w", encoding="utf-8") as source_file,
                trace_path.open("w", encoding="utf-8") as trace_file,
            ):
                source_file.write(
                    json.dumps(
                        {
                            "__header__": True,
                            "schema": (
                                "planetu.msgsource.nokov.v1"
                                if self.source_kind == "nokov"
                                else "planetu.msgsource.mocap.v1"
                            ),
                        }
                    )
                    + "\n"
                )
                trace_file.write(
                    json.dumps(
                        {
                            "__header__": True,
                            "schema": "planetu.trace.v1",
                        }
                    )
                    + "\n"
                )
                last_flush = time.monotonic()
                while self._running or self._queue:
                    with self._condition:
                        self._condition.wait_for(
                            lambda: bool(self._queue) or not self._running,
                            timeout=self.flush_interval_s,
                        )
                        item = self._queue.popleft() if self._queue else None
                    if item is not None:
                        kind, value = item
                        if kind == "source":
                            payload = self.source_serializer(value)
                            target = source_file
                        else:
                            payload = (
                                asdict(value) if is_dataclass(value) else dict(value)
                            )
                            payload["record_type"] = "trace"
                            target = trace_file
                        target.write(
                            json.dumps(
                                payload,
                                separators=(",", ":"),
                                ensure_ascii=True,
                                allow_nan=False,
                            )
                            + "\n"
                        )
                        self._records_written += 1
                    now = time.monotonic()
                    if now - last_flush >= self.flush_interval_s:
                        source_file.flush()
                        trace_file.flush()
                        last_flush = now
                source_file.flush()
                trace_file.flush()
        except BaseException as error:
            self._error = error
            self._running = False


class SessionRecordPublisher:
    """Send source/trace records to PlanetRecord without writing planner-local files."""

    def __init__(
        self,
        endpoint: str,
        *,
        robot: str,
        source_kind: str = "nokov",
        source_serializer=lambda value: value,
        queue_capacity: int = 16_384,
        flush_interval_s: float = 0.5,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not endpoint.startswith("@"):
            raise ValueError("PlanetRecord endpoint must be a Linux abstract address")
        self.endpoint = str(endpoint)
        self.session_dir = f"planetr:{self.endpoint}"
        self.robot = str(robot)
        self.source_kind = str(source_kind)
        self.source_serializer = source_serializer
        self.queue_capacity = int(queue_capacity)
        self.flush_interval_s = float(flush_interval_s)
        self.metadata = dict(metadata or {})
        self._condition = threading.Condition()
        self._queue: deque[tuple[str, object]] = deque()
        self._running = False
        self._thread: threading.Thread | None = None
        self._socket: socket.socket | None = None
        self._error: BaseException | None = None
        self._source_frames_enqueued = 0
        self._trace_frames_enqueued = 0
        self._records_written = 0
        self._records_dropped = 0

    @property
    def error(self) -> BaseException | None:
        return self._error

    def start(self) -> None:
        if self._running:
            return
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="planet_pingpong-planetr-publisher", daemon=True
        )
        self._thread.start()

    def record_source(self, snapshot) -> None:
        self._enqueue("source", snapshot)
        self._source_frames_enqueued += 1

    def record_trace(self, trace: object) -> None:
        self._enqueue("trace", trace)
        self._trace_frames_enqueued += 1

    def _enqueue(self, kind: str, value: object) -> None:
        if not self._running:
            return
        with self._condition:
            if len(self._queue) >= self.queue_capacity:
                self._queue.popleft()
                self._records_dropped += 1
            self._queue.append((kind, value))
            self._condition.notify()

    def stats(self) -> RecordingStats:
        with self._condition:
            depth = len(self._queue)
        return RecordingStats(
            session_dir=self.session_dir,
            source_frames_enqueued=self._source_frames_enqueued,
            trace_frames_enqueued=self._trace_frames_enqueued,
            records_written=self._records_written,
            records_dropped=self._records_dropped,
            queue_depth=depth,
            error="" if self._error is None else str(self._error),
        )

    def close(self, timeout_s: float = 2.0) -> None:
        self._running = False
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=max(float(timeout_s), 0.0))
            self._thread = None
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def _run(self) -> None:
        assert self._socket is not None
        address = "\0" + self.endpoint[1:]
        while self._running or self._queue:
            with self._condition:
                self._condition.wait_for(
                    lambda: bool(self._queue) or not self._running,
                    timeout=0.1,
                )
                item = self._queue.popleft() if self._queue else None
            if item is None:
                continue
            kind, value = item
            try:
                payload = (
                    self.source_serializer(value)
                    if kind == "source"
                    else (asdict(value) if is_dataclass(value) else dict(value))
                )
                envelope = {
                    "schema": "planetr.ingress.v1",
                    "kind": kind,
                    "robot": self.robot,
                    "metadata": (
                        {"source_kind": self.source_kind, **self.metadata}
                        if self._records_written == 0
                        else {}
                    ),
                    "payload": payload,
                }
                encoded_json = json.dumps(
                    envelope,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("utf-8")
                encoded = b"PRR1" + zlib.compress(encoded_json, level=1)
                if len(encoded) > 60_000:
                    raise ValueError("PlanetRecord record exceeds 60000 bytes")
                self._socket.sendto(encoded, address)
                self._records_written += 1
            except (BlockingIOError, OSError, TypeError, ValueError) as error:
                self._records_dropped += 1
                if not isinstance(error, (BlockingIOError, OSError)):
                    self._error = error


__all__ = ["RecordingStats", "SessionRecorder", "SessionRecordPublisher"]
