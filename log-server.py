import socket

from loguru import logger

logger.add("logs/remote_qsend.log", rotation="500 MB")

HOST = "0.0.0.0"
PORT = 9999


def start_server():
    """Starts a server to receive logs from clients."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        logger.info(f"Receiver started. Listening on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            with conn:
                logger.info(f"Connection accepted from {addr}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break

                    decoded_message = data.decode("utf-8")

                    logger.opt(raw=True).info(decoded_message)


if __name__ == "__main__":
    start_server()
