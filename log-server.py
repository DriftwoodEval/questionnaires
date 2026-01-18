import socket
import sys

from loguru import logger

HOST = "0.0.0.0"
PORT = 9999

# Keep track of handlers to avoid adding them multiple times
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
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break

                    try:
                        decoded_data = data.decode("utf-8")
                        # Data might contain multiple log entries separated by newlines
                        for line in decoded_data.splitlines():
                            if not line.strip():
                                continue

                            if ":" in line:
                                app_name, message = line.split(":", 1)
                                log_to_app(app_name, message)
                            else:
                                # Fallback if no app_name is provided
                                log_to_app("unknown", line)
                    except Exception as e:
                        logger.error(f"Error processing data: {e}")


if __name__ == "__main__":
    start_server()
