FROM selenium/standalone-chrome AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen

FROM base as qreceive
COPY shared_utils.py qreceive.py .
CMD ["uv", "run", "qreceive.py"]

FROM base as qmail
COPY shared_utils.py qmail.py .
CMD ["uv", "run", "qmail.py"]
