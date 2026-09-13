"""Receive planner records and A3 onboard telemetry into one PC session."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import selectors
import socket
import time
import zlib

from cadence_config import load_config, validate_keys


from .protocol import A3DebugReassembler
from .recorder import PlanetRRecorder


def _debug_stream(frame: dict) -> str:
    """Keep old/missing backend frames compatible with onboard recordings."""
    return "sim2sim" if frame.get("runtime_backend") == "mujoco" else "onboard"


def _unix_address(endpoint: str) -> str:
    if not endpoint.startswith("@"):
        raise ValueError("PlanetRecord endpoint must start with @")
    return "\0" + endpoint[1:]


def _load(path: str | Path) -> dict:
    resolved = load_config(
        path,
        allowed={"version", "onboard", "planner", "planetd", "recording"},
        required={"version", "onboard", "planner", "recording"},
    )
    raw = resolved.data
    if int(raw.get("version", 0)) != 1:
        raise ValueError("PlanetRecord config version must be 1")
    validate_keys(raw["onboard"], {"bind_host", "port"}, required={"bind_host", "port"})
    validate_keys(raw["planner"], {"endpoint"}, required={"endpoint"})
    validate_keys(raw["recording"], {"directory", "flush_interval_s"}, required={"directory"})
    if "planetd" in raw:
        validate_keys(raw["planetd"], {"onboard_endpoint"})
    raw["_source"] = resolved.source
    return raw


def run(
    config: dict,
    duration_s: float = 0.0,
    recording_directory: str | Path | None = None,
) -> int:
    onboard = config["onboard"]
    planner = config["planner"]
    recording = config["recording"]
    forward_endpoint = str(config.get("planetd", {}).get("onboard_endpoint", ""))
    recorder = PlanetRRecorder(
        recording_directory or recording["directory"],
        float(recording.get("flush_interval_s", 0.5)),
    )
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind((str(onboard["bind_host"]), int(onboard["port"])))
    udp.setblocking(False)
    unix = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    unix.bind(_unix_address(str(planner["endpoint"])))
    unix.setblocking(False)
    forward = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    forward.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(udp, selectors.EVENT_READ, "onboard")
    selector.register(unix, selectors.EVENT_READ, "planner")
    reassembler = A3DebugReassembler()
    planner_invalid = forward_dropped = 0
    started = time.monotonic()
    last_report = started
    print(
        f"planetr session={recorder.session_dir} "
        f"onboard={onboard['bind_host']}:{onboard['port']} "
        f"planner={planner['endpoint']}"
    )
    try:
        while duration_s <= 0 or time.monotonic() - started < duration_s:
            for key, _mask in selector.select(timeout=0.2):
                if key.data == "onboard":
                    datagram, address = udp.recvfrom(65_535)
                    frame = reassembler.feed(datagram, address[0])
                    if frame is None:
                        continue
                    recorder.record(_debug_stream(frame), frame)
                    if forward_endpoint:
                        try:
                            forward.sendto(
                                json.dumps(frame, separators=(",", ":")).encode(
                                    "utf-8"
                                ),
                                _unix_address(forward_endpoint),
                            )
                        except OSError:
                            forward_dropped += 1
                else:
                    datagram = unix.recv(65_535)
                    try:
                        if not datagram.startswith(b"PRR1"):
                            raise ValueError("invalid PlanetRecord record magic")
                        envelope = json.loads(
                            zlib.decompress(datagram[4:]).decode("utf-8")
                        )
                        if envelope.get("schema") != "planetr.ingress.v1":
                            raise ValueError("invalid PlanetRecord ingress schema")
                        recorder.record(
                            str(envelope["kind"]),
                            dict(envelope["payload"]),
                            dict(envelope.get("metadata", {})),
                        )
                    except (
                        KeyError,
                        TypeError,
                        ValueError,
                        UnicodeError,
                        json.JSONDecodeError,
                        zlib.error,
                    ):
                        planner_invalid += 1
            now = time.monotonic()
            if now - last_report >= 1.0:
                counts = recorder.counts
                print(
                    f"[PLANETR] onboard={counts['onboard']} "
                    f"sim2sim={counts['sim2sim']} "
                    f"planner_source={counts['source']} trace={counts['trace']} "
                    f"udp_invalid={reassembler.invalid} "
                    f"udp_expired={reassembler.expired} "
                    f"planner_invalid={planner_invalid} forward_drop={forward_dropped}"
                )
                last_report = now
    except KeyboardInterrupt:
        return 0
    finally:
        selector.close()
        udp.close()
        unix.close()
        forward.close()
        recorder.close(
            {
                "errors": planner_invalid + reassembler.invalid,
                "gaps": reassembler.expired,
                "udp_invalid": reassembler.invalid,
                "udp_expired": reassembler.expired,
                "planner_invalid": planner_invalid,
                "planetd_forward_dropped": forward_dropped,
            }
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/common/planetr.yaml")
    parser.add_argument(
        "--dir",
        help="recording root override, for example recordings/sim2sim",
    )
    parser.add_argument("--duration-s", type=float, default=0.0)
    args = parser.parse_args(argv)
    return run(_load(args.config), args.duration_s, args.dir)


__all__ = ["_debug_stream", "main", "run"]
