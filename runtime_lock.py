"""One MCP printer-session owner per user, across HTTP and stdio transports."""
from __future__ import annotations

import fcntl
import os
from pathlib import Path

_fd: int | None = None


def claim() -> None:
    """Hold an OS lock until process exit; stale PID files cannot keep it locked."""
    global _fd
    if _fd is not None:
        return
    directory = Path.home() / ".bambu-mcp"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(directory / "runtime.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
    except BlockingIOError:
        os.close(fd)
        raise RuntimeError("Bambu MCP already owns this user's printer connection; use its shared HTTP endpoint") from None
    except BaseException:
        os.close(fd)
        raise
    _fd = fd
