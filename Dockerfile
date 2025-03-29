FROM ghcr.io/astral-sh/uv:bookworm-slim

WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen

COPY shared_utils.py qreceive.py .

CMD ["uv", "run", "qreceive.py"]
