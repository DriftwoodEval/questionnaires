FROM selenium/standalone-chrome
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen

COPY shared_utils.py qreceive.py .

CMD ["uv", "run", "qreceive.py"]
