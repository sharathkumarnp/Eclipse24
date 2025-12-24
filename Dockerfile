# ==== Builder Stage ======
FROM <your_image_base>/<platform-base:0.2.2> AS builder

RUN --mount=type=secret,id=REPO_USER,env=USER \
    --mount=type=secret,id=REPO_PASS,env=PASS \
    if [ -n "$USER" ] && [ -n "$PASS" ]; then \
        export PIP_INDEX_URL="<your_index_path>"; \
    else \
        export PIP_INDEX_URL="<your_index_path>"; \
    fi && \
    pip install --upgrade pip && \
    pip install poetry==2.0.0

WORKDIR /app/eclipse24-service

# copy everything (pyproject, src, configs, etc.)
COPY . /app/eclipse24-service

# build wheel via poetry (expects [tool.poetry] in pyproject.toml)
RUN poetry build


# ==== Runtime Stage ======
FROM <base>/<your-base-image:0.2.2>

WORKDIR /app

RUN python --version

# copy built wheel from builder stage
COPY --from=builder /app/eclipse24-service/dist/*.whl /app/

# install from internal index (needed for narcissus-client etc.)
RUN --mount=type=secret,id=REPO_USER,env=REPO_USER \
    --mount=type=secret,id=REPO_PASS,env=REPO_PASS \
    if [ -n "$REPO_USER" ] && [ -n "$REPO_PASS" ]; then \
        export PIP_INDEX_URL="<your_index_path>"; \
    else \
        export PIP_INDEX_URL="<your_index_path>"; \
    fi && \
    pip install --no-cache-dir /app/*.whl && \
    rm -rf /app/*.whl

# configs (rules.yaml, queues.yaml) – used by RULES_PATH / QUEUES_PATH
COPY src/eclipse24/configs /app/config

# entrypoint – now includes audit service as well
COPY <<'EOF' /usr/local/bin/entrypoint.sh
#!/usr/bin/env sh
set -e

export RULES_PATH="${RULES_PATH:-/app/config/rules.yaml}"
export QUEUES_PATH="${QUEUES_PATH:-/app/config/queues.yaml}"

echo "[entrypoint] SERVICE=${SERVICE:-api}"
echo "[entrypoint] RULES_PATH=$RULES_PATH"
echo "[entrypoint] QUEUES_PATH=$QUEUES_PATH"
echo "[entrypoint] KAFKA_BROKERS=${KAFKA_BROKERS:-<unset>}"
echo "[entrypoint] UMBRELLA_USER=${UMBRELLA_USER:-<unset>}"
echo "[entrypoint] NEWRELIC_API_KEY=${NEWRELIC_API_KEY:-<unset>}"
echo "[entrypoint] PERSIST_TO_DB=${PERSIST_TO_DB:-false}"

case "${SERVICE:-api}" in
  api)
    exec uvicorn eclipse24.services.api.main:app --host 0.0.0.0 --port "${PORT:-8000}"
    ;;
  approver)
    exec python -m eclipse24.services.approver.main
    ;;
  validator)
    exec python -m eclipse24.services.validator.main
    ;;
  reminder)
    exec python -m eclipse24.services.reminder.main
    ;;
  expiry_watcher|expiry)
    exec python -m eclipse24.services.expiry_watcher.main
    ;;
  blocker)
    exec python -m eclipse24.services.blocker.main
    ;;
  poller)
    exec python -m eclipse24.services.poller.main
    ;;
  audit)
    exec python -m eclipse24.services.audit.main
    ;;
  *)
    echo "Unknown SERVICE=${SERVICE}" >&2
    exit 2
    ;;
esac
EOF

RUN chmod +x /usr/local/bin/entrypoint.sh

# fix ownership for runtime user
RUN chown -R sre-operator:sre-operator /app

USER sre-operator

EXPOSE 8010

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]