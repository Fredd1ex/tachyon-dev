"""Private serialized control protocol; never read control from stdout/stderr."""
import asyncio
import json
import os
import socket
import struct
import threading
from IPython.core.interactiveshell import InteractiveShell

LIMIT = 1024 * 1024
control = socket.socket(socket.AF_UNIX)
control.connect(os.environ.pop("GHOST_PYTHON_SOCKET"))


def receive():
    def exact(n):
        data = bytearray()
        while len(data) < n:
            part = control.recv(n - len(data))
            if not part:
                raise EOFError("bridge closed")
            data.extend(part)
        return data
    size = struct.unpack("!I", exact(4))[0]
    if not 0 < size <= LIMIT:
        raise ValueError("invalid frame size")
    return json.loads(exact(size))


def frame(value):
    data = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
    if len(data) > LIMIT:
        raise ValueError("frame too large")
    return struct.pack("!I", len(data)) + data


def send(value):
    control.sendall(frame(value))


request = None
sequence = 0
owner = threading.get_ident()
busy = False


def hostcall(kind, **fields):
    global sequence
    if busy:
        raise RuntimeError("concurrent hostcalls are unsupported; await the previous call first")
    if request is None or threading.get_ident() != owner:
        raise RuntimeError("hostcalls require an active cell on the kernel thread")
    data = frame(dict(kind=kind, request_id=request, id=sequence + 1, **fields))
    sequence += 1
    try:
        control.sendall(data)
        reply = receive()
        if reply["request_id"] != request or reply["id"] != sequence:
            raise RuntimeError("mismatched host reply")
    except BaseException:
        control.close()
        raise
    if reply["outcome"] != "ok":
        raise RuntimeError(reply["error"])
    return reply["value"]


async def async_hostcall(**fields):
    global busy, sequence
    if request is None or threading.get_ident() != owner:
        raise RuntimeError("hostcalls require an active cell on the kernel thread")
    if busy:
        raise RuntimeError("concurrent hostcalls are unsupported; await the previous call first")
    # Local encoding failures have not started a transaction or consumed an ID.
    data = frame(dict(kind="call", request_id=request, id=sequence + 1, **fields))
    busy = True
    try:
        sequence += 1
        control.setblocking(False)
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(control, data)
        async def exact(n):
            data = bytearray()
            while len(data) < n:
                part = await loop.sock_recv(control, n - len(data))
                if not part:
                    raise EOFError("bridge closed")
                data.extend(part)
            return data
        size = struct.unpack("!I", await exact(4))[0]
        if not 0 < size <= LIMIT:
            raise ValueError("invalid frame size")
        reply = json.loads(await exact(size))
        if reply["request_id"] != request or reply["id"] != sequence:
            raise RuntimeError("mismatched host reply")
    except BaseException:
        # A cancelled transaction cannot safely leave an unread reply for a cell.
        control.close()
        raise
    finally:
        if control.fileno() != -1:
            control.setblocking(True)
        busy = False
    if reply["outcome"] != "ok":
        raise RuntimeError(reply["error"])
    return reply["value"]


class Proxy:
    def __init__(self, info, asynchronous):
        self.package = info["package"]
        self.asynchronous = asynchronous
        self.guidance = info["guidance"]
        self.schemas = {s["name"]: s["parameters"] for s in info["schemas"]}
        self.methods = info["methods"]

    def __getattr__(self, name):
        if name not in self.methods:
            raise AttributeError(name)
        method = self.methods[name]
        def call(**arguments):
            arguments = dict(**method["input"], **arguments)
            if self.asynchronous or method["asynchronous"]:
                return async_hostcall(tool=method["tool"], input=arguments)
            return hostcall("call", tool=method["tool"], input=arguments)
        return call


def require(name, *, asynchronous=False):
    return Proxy(hostcall("require", name=name), asynchronous)


shell = InteractiveShell.instance()
shell.user_ns["require"] = require
while True:
    cell = receive()
    request = cell["request_id"]
    sequence = 0
    for info in cell.get("preloaded", []):
        shell.user_ns.setdefault(info["package"], Proxy(info, False))
    # FD-level capture includes !shell, os.write and Python streams. A reader
    # drains continuously but retains only a bounded prefix, never control data.
    read_fd, write_fd = os.pipe()
    saved = [os.dup(1), os.dup(2)]
    output = bytearray()
    overflow = [False]
    def drain():
        while True:
            data = os.read(read_fd, 8192)
            if not data:
                break
            room = 64 * 1024 - len(output)
            output.extend(data[:room])
            overflow[0] |= len(data) > room
        os.close(read_fd)
    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    os.dup2(write_fd, 1)
    os.dup2(write_fd, 2)
    os.close(write_fd)
    try:
        result = shell.run_cell(cell["code"], store_history=False)
        if busy:
            raise RuntimeError("background hostcalls are unsupported")
        import sys
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        for fd, original in zip((1, 2), saved):
            os.dup2(original, fd)
            os.close(original)
    reader.join(timeout=0.2)
    if reader.is_alive():
        raise RuntimeError("background output is unsupported")
    send(dict(kind="done", request_id=request, success=result.success,
              output=output.decode("utf-8", errors="replace"), truncated=overflow[0]))
    request = None
