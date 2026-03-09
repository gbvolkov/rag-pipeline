# syntax=docker/dockerfile:1.7
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_CACHE_DIR=/root/.cache/uv

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock .python-version /app/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project \
    && uv pip install --python /app/.venv/bin/python "psycopg[binary]>=3.2.0"

RUN /app/.venv/bin/python -m playwright install --with-deps chromium

COPY . /app

EXPOSE 8007

CMD ["/app/.venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8007"]
