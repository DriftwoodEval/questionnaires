FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

FROM builder AS qreceive

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium=147.0.7727.137-1~deb12u1 \
    chromium-driver=147.0.7727.137-1~deb12u1 \
    chromium-common=147.0.7727.137-1~deb12u1 \
    curl \
    && apt-mark hold chromium chromium-driver chromium-common \
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
    CRON_SCHEDULE="0 7,9,11,13,15,17,19 * * *" \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint-qreceive.sh"]

FROM builder AS log-server

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .

ENV TZ=America/New_York \
    PYTHONUNBUFFERED=1

CMD ["uv", "run", "log-server.py"]
