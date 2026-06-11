import socket
import sys

from loguru import logger

HOST = "0.0.0.0"
PORT = 9999

app_handlers = {}


def log_to_app(app_name, message):
    """Logs a message to an app-specific file."""
    if app_name not in app_handlers:
        log_file = f"logs/remote_{app_name}.log"
        app_handlers[app_name] = logger.add(
            log_file,
            rotation="500 MB",
            filter=lambda record: record["extra"].get("app") == app_name,
            format="{message}",
        )
        logger.info(f"Created new log handler for app: {app_name}")
    logger.bind(app=app_name).info(message)


def parse_line(line, last_app_name):
    """Parse a complete line and return the (app_name, message) pair."""
    if ":" in line:
        possible_app_name, message = line.split(":", 1)
        if " " not in possible_app_name and len(possible_app_name) < 32:
            return possible_app_name, message
    return last_app_name, line


def handle_connection(conn):
    """Handle a single client connection, buffering until complete lines arrive."""
    last_app_name = "unknown"
    buffer = ""
    while True:
        data = conn.recv(4096)
        if not data:
            break
        try:
            buffer += data.decode("utf-8")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                last_app_name, message = parse_line(line, last_app_name)
                log_to_app(last_app_name, message)
        except Exception as e:
            logger.error(f"Error processing data: {e}")


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
                handle_connection(conn)


if __name__ == "__main__":
    start_server()
