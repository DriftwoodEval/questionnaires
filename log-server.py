import hmac
import os
import socket
import sys
from pathlib import Path

from loguru import logger

HOST = "0.0.0.0"
PORT = 9999

SHARED_SECRET = os.environ.get("LOG_SERVER_SHARED_SECRET")
if not SHARED_SECRET:
    logger.error("LOG_SERVER_SHARED_SECRET is not set. Refusing to start.")
    sys.exit(1)

# (handler_id, inode) per app, so we can detect if the file was deleted or
# replaced out from under us (e.g. external cleanup) and reopen it instead
# of silently writing to an unlinked file until the process is restarted.
app_handlers = {}


def log_to_app(app_name, message):
    """Logs a message to an app-specific file."""
    log_file = Path(f"logs/remote_{app_name}.log")
    handler_id, inode = app_handlers.get(app_name, (None, None))
    current_inode = log_file.stat().st_ino if log_file.exists() else None

    if handler_id is None or current_inode != inode:
        if handler_id is not None:
            logger.remove(handler_id)
        handler_id = logger.add(
            log_file,
            rotation="500 MB",
            filter=lambda record: record["extra"].get("app") == app_name,
            format="{message}",
        )
        app_handlers[app_name] = (handler_id, log_file.stat().st_ino)
        logger.info(f"Created new log handler for app: {app_name}")

    logger.bind(app=app_name).info(message)


def parse_line(line, last_app_name):
    """Parse a complete line and return the (app_name, message) pair."""
    if ":" in line:
        possible_app_name, message = line.split(":", 1)
        if " " not in possible_app_name and len(possible_app_name) < 32:
            return possible_app_name, message
    return last_app_name, line


def _authenticate(conn, addr) -> tuple[bool, bytes]:
    """Read the connection's first line and check it against the shared secret.

    Every connection must open with "AUTH:<secret>\n" before any log lines are
    accepted. Returns (False, b"") (and logs, without the secret itself) on
    anything else: wrong secret, no secret, or the connection closing before a
    full line arrives. On success, also returns whatever bytes arrived after
    the auth line's "\n" in the same recv() call, so the caller doesn't drop
    them - the client isn't guaranteed to send the auth line in its own packet.
    """
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            return False, b""
        buffer += chunk
        if len(buffer) > 4096:
            logger.warning(f"Auth line too long from {addr}")
            return False, b""

    line, _, rest = buffer.partition(b"\n")
    prefix = b"AUTH:"
    if not line.startswith(prefix):
        logger.warning(f"Connection from {addr} skipped the auth handshake")
        return False, b""

    token = line[len(prefix) :].decode("utf-8", errors="replace")
    assert SHARED_SECRET is not None  # checked at startup
    if not hmac.compare_digest(token, SHARED_SECRET):
        logger.warning(f"Auth failed for connection from {addr}")
        return False, b""
    return True, rest


def handle_connection(conn, addr):
    """Handle a single client connection, buffering until complete lines arrive."""
    authenticated, leftover = _authenticate(conn, addr)
    if not authenticated:
        return

    last_app_name = "unknown"
    buffer = leftover.decode("utf-8", errors="replace")
    while True:
        # A complete line can already be sitting in `buffer` before the next
        # recv() (leftover from the auth packet, or a line the previous
        # iteration didn't fully drain) - process it before blocking on new
        # data, and again after the loop ends so a final line sent right
        # before the client closes the connection isn't dropped.
        try:
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                last_app_name, message = parse_line(line, last_app_name)
                log_to_app(last_app_name, message)
        except Exception as e:
            logger.error(f"Error processing data: {e}")

        data = conn.recv(4096)
        if not data:
            break
        try:
            buffer += data.decode("utf-8")
        except Exception as e:
            logger.error(f"Error processing data: {e}")

    if buffer.strip():
        last_app_name, message = parse_line(buffer.strip(), last_app_name)
        log_to_app(last_app_name, message)


def start_server():
    """Starts a server to receive logs from clients."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((HOST, PORT))
        except OSError as e:
            logger.error(f"Failed to bind to {HOST}:{PORT}: {e}")
            sys.exit(1)
        s.listen()
        logger.info(f"Receiver started. Listening on {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            with conn:
                logger.info(f"Connection accepted from {addr}")
                handle_connection(conn, addr)


if __name__ == "__main__":
    start_server()
