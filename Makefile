.PHONY: install dev run-api lint test

# 1) install deps (local)
install:
\tpython -m venv .venv
\t. .venv/bin/activate && pip install -U pip
\t. .venv/bin/activate && pip install -e ".[dev]"

# 2) run API locally (will look for src/eclipse24/services/api/main.py:app)
run-api:
\t. .venv/bin/activate && uvicorn eclipse24.services.api.main:app --reload --host 0.0.0.0 --port 8000

# 3) run tests
test:
\t. .venv/bin/activate && pytest -q

# 4) lint (optional)
lint:
\t. .venv/bin/activate && ruff src tests