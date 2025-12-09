# Build stage
FROM python:3.12.8 AS builder

WORKDIR /usr/src/app

RUN apt-get -y update && \
    apt-get install -y curl gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/0.9.16/install.sh | sh
ENV PATH="/root/.local/bin/:$PATH"

COPY pyproject.toml uv.lock ./

# Export lockfile to requirements.txt, then sync to system Python
# RUN uv export --no-dev --frozen -o requirements.txt && \
#     uv pip sync --system --no-cache requirements.txt

# Use pip install (additive) - doesn't remove existing packages
RUN uv pip install --system --no-cache -r pyproject.toml

# Runtime stage
FROM python:3.12.8

WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install only runtime dependencies
RUN apt-get -y update && \
    apt-get install -y wget gnupg lsb-release ca-certificates && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg && \
    apt-get -y update && \
    TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bash \
        git \
        libpq-dev \
        postgresql-client-15 \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Copy installed dependencies from builder
COPY --from=builder /root/.local /root/.local

# Copy installed Python packages from system site-packages
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

ENV PATH="/root/.local/bin/:$PATH"

COPY . .

RUN git config --global --add safe.directory /workspace

EXPOSE 8000

RUN chmod +x /usr/src/app/scripts/wait_for_db.sh
RUN chmod +x /usr/src/app/scripts/entrypoint.sh

ENTRYPOINT ["/usr/src/app/scripts/entrypoint.sh"]