FROM selenium/standalone-chrome
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen

COPY qsend.py .
