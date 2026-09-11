"""Decoder/reassembler for the versioned A3DB v1 UDP protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import struct
import time
import zlib


MAGIC = b"A3DB"
VERSION = 1
MESSAGE_FRAME = 1
HEADER = struct.Struct("<4sBBBBIQQHHI")


@dataclass(slots=True)
class _Assembly:
    chunk_count: int
    checksum: int
    updated_s: float
    chunks: dict[int, bytes] = field(default_factory=dict)


class A3DebugReassembler:
    def __init__(self, timeout_s: float = 1.0) -> None:
        self.timeout_s = float(timeout_s)
        self._frames: dict[tuple[str, int, int], _Assembly] = {}
        self.completed = 0
        self.invalid = 0
        self.expired = 0

    def feed(self, datagram: bytes, source_host: str, now_s: float | None = None):
        now = time.monotonic() if now_s is None else float(now_s)
        self._expire(now)
        if len(datagram) < HEADER.size:
            self.invalid += 1
            return None
        (
            magic,
            version,
            message_type,
            _flags,
            _reserved,
            session_id,
            frame_seq,
            captured_mono_ns,
            chunk_index,
            chunk_count,
            checksum,
        ) = HEADER.unpack_from(datagram)
        if (
            magic != MAGIC
            or version != VERSION
            or message_type != MESSAGE_FRAME
            or chunk_count < 1
            or chunk_index >= chunk_count
        ):
            self.invalid += 1
            return None
        key = (str(source_host), int(session_id), int(frame_seq))
        assembly = self._frames.get(key)
        if assembly is None:
            assembly = _Assembly(int(chunk_count), int(checksum), now)
            self._frames[key] = assembly
        if assembly.chunk_count != chunk_count or assembly.checksum != checksum:
            self.invalid += 1
            self._frames.pop(key, None)
            return None
        assembly.updated_s = now
        assembly.chunks[int(chunk_index)] = datagram[HEADER.size :]
        if len(assembly.chunks) != assembly.chunk_count:
            return None
        payload = b"".join(assembly.chunks[index] for index in range(chunk_count))
        self._frames.pop(key, None)
        if zlib.crc32(payload) & 0xFFFFFFFF != checksum:
            self.invalid += 1
            return None
        try:
            frame = json.loads(zlib.decompress(payload).decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            self.invalid += 1
            return None
        if frame.get("schema") != "agi3.onboard_debug.v1":
            self.invalid += 1
            return None
        if (
            int(frame.get("session_id", -1)) != session_id
            or int(frame.get("frame_seq", -1)) != frame_seq
            or int(frame.get("captured_mono_ns", -1)) != captured_mono_ns
        ):
            self.invalid += 1
            return None
        self.completed += 1
        return frame

    def _expire(self, now_s: float) -> None:
        expired = [
            key
            for key, value in self._frames.items()
            if now_s - value.updated_s > self.timeout_s
        ]
        for key in expired:
            self._frames.pop(key, None)
        self.expired += len(expired)


__all__ = ["A3DebugReassembler", "HEADER", "MAGIC", "VERSION"]
