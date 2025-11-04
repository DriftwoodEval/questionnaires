FROM selenium/standalone-chrome:4 AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libffi-dev \
    libgdal-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --chmod=555 ./docker/scripts/*.sh .

ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.34/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=e8631edc1775000d119b70fd40339a7238eece14 \
    SUPERCRONIC=supercronic-linux-amd64

RUN curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
    && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# ---
FROM base AS qreceive

COPY qreceive.py ./
COPY utils ./utils/

ENV TZ=America/New_York \
    CRON_SCHEDULE="0 13 * * *"

ENTRYPOINT ["/app/entrypoint-qreceive.sh"]
