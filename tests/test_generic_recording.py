import json
import socket
from dataclasses import replace
from queue import Queue
from threading import Lock
import pytest
from planetr_format import RecordEnvelope
from planetr_client import RecordClient
from planetr.session import SessionWriter, replay


def frame(sequence=0, **kwargs):
    return RecordEnvelope(
        "test-session",
        "robot/state",
        "robot.state.v1",
        "test",
        sequence,
        sequence * 1000,
        {"q": [0.0, 1.0]},
        **kwargs,
    )


def test_record_replay_preserves_raw_schema_and_integrity(tmp_path):
    writer = SessionWriter(
        tmp_path,
        {"robot/state": {"schema": "robot.state.v1", "filename": "state.jsonl"}},
        session_id="test-session",
    )
    for i in range(3):
        writer.write(frame(i))
    writer.close({"gaps": 1})
    rows = list(replay(writer.session_dir))
    assert [r["sequence"] for r in rows] == [0, 1, 2]
    assert rows[0]["payload"] == {"q": [0.0, 1.0]}
    assert not json.loads((writer.session_dir / "meta.json").read_text())["complete"]
    with pytest.raises(RuntimeError):
        writer.write(frame())


def test_schema_and_clock_domains_are_checked(tmp_path):
    writer = SessionWriter(
        tmp_path,
        {"robot/state": {"schema": "robot.state.v1", "filename": "state.jsonl"}},
        session_id="test-session",
    )
    with pytest.raises(ValueError, match="schema"):
        writer.write(replace(frame(), schema="wrong.v1"))
    writer.write(frame())
    writer.write(replace(frame(1), producer="remote"))
    writer.close()
    with pytest.raises(ValueError, match="clock domain"):
        list(replay(writer.session_dir, speed=100))


@pytest.mark.parametrize(
    "filename", ["../state.jsonl", "/tmp/state.jsonl", "state.txt"]
)
def test_stream_path_validation(tmp_path, filename):
    with pytest.raises(ValueError):
        SessionWriter(tmp_path, {"x": {"schema": "x.v1", "filename": filename}})


def test_client_udp_roundtrip_and_queue_pressure():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.settimeout(1)
        client = RecordClient(sock.getsockname())
        assert client.publish(frame())
        actual = RecordEnvelope.decode(sock.recv(65535))
        client.close()
        assert actual == frame() and client.sent == 1
    # Deterministic pressure test without a concurrently draining worker.
    client = object.__new__(RecordClient)
    client.lock = Lock()
    client.running = True
    client.dropped = 0
    client.queue = Queue(1)
    assert (
        client.publish(frame()) and not client.publish(frame(1)) and client.dropped == 1
    )


@pytest.mark.parametrize("data", [b"{}", b"not json", b"[]", b"x" * 65508])
def test_malformed_envelopes(data):
    with pytest.raises((ValueError, TypeError)):
        RecordEnvelope.decode(data)
