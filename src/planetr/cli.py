"""Independent recorder and replay command."""

import argparse
import json
from pathlib import Path
from queue import Queue, Full, Empty
from threading import Thread
import socket
import time
from planet_config import load_config, validate_keys
from planetr_format import RecordEnvelope
from .session import SessionWriter, replay


def receive(config, duration_s=0):
    validate_keys(
        config,
        {
            "version",
            "bind",
            "streams",
            "directory",
            "session_id",
            "queue_capacity",
            "metadata",
        },
        required={"version", "bind", "streams", "directory"},
    )
    if config["version"] != 1:
        raise ValueError("unsupported config version")
    capacity = int(config.get("queue_capacity", 2048))
    if capacity < 1:
        raise ValueError("queue capacity must be positive")
    queue = Queue(capacity)
    stats = {"dropped": 0, "errors": 0, "gaps": 0}
    running = True
    sessions = {}
    last = {}

    def writer():
        while running or not queue.empty():
            try:
                e = queue.get(timeout=0.05)
            except Empty:
                continue
            try:
                if config.get("session_id") and e.session_id != config["session_id"]:
                    raise ValueError("unexpected session")
                if e.session_id not in sessions:
                    sessions[e.session_id] = SessionWriter(
                        config["directory"],
                        config["streams"],
                        session_id=e.session_id,
                        metadata=config.get("metadata"),
                    )
                key = (e.session_id, e.producer, e.stream)
                if key in last and e.sequence > last[key] + 1:
                    stats["gaps"] += e.sequence - last[key] - 1
                last[key] = max(last.get(key, -1), e.sequence)
                sessions[e.session_id].write(e)
            except (OSError, ValueError, TypeError):
                stats["errors"] += 1
            finally:
                queue.task_done()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((config["bind"]["host"], int(config["bind"]["port"])))
    sock.settimeout(0.1)
    worker = Thread(target=writer, name="planetr-writer", daemon=True)
    worker.start()
    started = time.monotonic()
    print("PlanetRecord listening on " + str(sock.getsockname()), flush=True)
    try:
        while duration_s <= 0 or time.monotonic() - started < duration_s:
            try:
                packet = sock.recv(65535)
            except socket.timeout:
                continue
            try:
                queue.put_nowait(RecordEnvelope.decode(packet))
            except Full:
                stats["dropped"] += 1
            except (ValueError, TypeError, KeyError):
                stats["errors"] += 1
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        running = False
        worker.join(5)
        if worker.is_alive():
            raise TimeoutError("record writer did not drain")
        for session in sessions.values():
            session.close(stats)
    print(json.dumps(stats))
    return 0 if not stats["errors"] else 1


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    run = sub.add_parser("record")
    run.add_argument("--config", default="pkg://planetr/data/default.yaml")
    run.add_argument("--duration-s", type=float, default=0)
    run.add_argument("--check", action="store_true")
    rep = sub.add_parser("replay")
    rep.add_argument("session")
    rep.add_argument("--speed", type=float, default=0)
    legacy = sub.add_parser("legacy")
    legacy.add_argument("--config", required=True)
    legacy.add_argument("--duration-s", type=float, default=0)
    legacy.add_argument("--dir")
    args = p.parse_args(argv)
    if args.action == "replay":
        for row in replay(args.session, speed=args.speed):
            print(json.dumps(row))
        return 0
    if args.action == "legacy":
        from .legacy_runtime import _load, run

        return run(_load(args.config), args.duration_s, args.dir)
    cfg = load_config(args.config).data
    if args.check:
        validate_keys(
            cfg,
            {
                "version",
                "bind",
                "streams",
                "directory",
                "session_id",
                "queue_capacity",
                "metadata",
            },
            required={"version", "bind", "streams", "directory"},
        )
        print("PlanetRecord configuration valid")
        return 0
    return receive(cfg, args.duration_s)
