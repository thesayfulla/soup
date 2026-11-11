FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.8.3

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --all-groups --no-install-project

COPY . .
