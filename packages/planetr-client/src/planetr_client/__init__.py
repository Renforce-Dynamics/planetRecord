"""Bounded asynchronous UDP recording client. Disk and networking stay off control ticks."""

from queue import Queue, Empty, Full
from threading import Thread, Lock
import socket
import time
from planetr_format import RecordEnvelope


class RecordClient:
    def __init__(self, address=("127.0.0.1", 50571), *, capacity=2048):
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.address = address
        self.queue = Queue(capacity)
        self.running = True
        self.lock = Lock()
        self.sent = 0
        self.dropped = 0
        self.errors = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(0.1)
        self.thread = Thread(target=self._run, name="planetr-client", daemon=True)
        self.thread.start()

    def publish(self, envelope):
        if not isinstance(envelope, RecordEnvelope):
            raise TypeError("expected RecordEnvelope")
        with self.lock:
            if not self.running:
                self.dropped += 1
                return False
            try:
                self.queue.put_nowait(envelope)
                return True
            except Full:
                self.dropped += 1
                return False

    def _run(self):
        while self.running or not self.queue.empty():
            try:
                value = self.queue.get(timeout=0.05)
            except Empty:
                continue
            try:
                data = value.encode()
                if len(data) > 65507:
                    raise ValueError("record exceeds UDP datagram limit")
                self.socket.sendto(data, self.address)
                self.sent += 1
            except (OSError, ValueError, TypeError):
                self.errors += 1
            finally:
                self.queue.task_done()

    def close(self, timeout=2):
        with self.lock:
            self.running = False
        self.thread.join(timeout)
        if self.thread.is_alive():
            raise TimeoutError("record client did not drain")
        self.socket.close()


__all__ = ["RecordClient", "RecordEnvelope"]
