FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

FROM builder AS qreceive

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.34/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=e8631edc1775000d119b70fd40339a7238eece14 \
    SUPERCRONIC=supercronic-linux-amd64

RUN curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
    && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY --chmod=555 ./docker/scripts/*.sh .
COPY . .

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    TZ=America/New_York \
    CRON_SCHEDULE="0 13 * * *" \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint-qreceive.sh"]

FROM builder AS config-server

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .

ENV TZ=America/New_York \
    PYTHONUNBUFFERED=1

CMD ["uv run config-server.py"]
