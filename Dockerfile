FROM python:3.12-slim

WORKDIR /app

# No extra system dependencies needed for asyncpg (pure Python)

# Install poetry
RUN pip install poetry && poetry config virtualenvs.in-project true

# Copy dependency files
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root

# Copy application
COPY . .

EXPOSE 8000

CMD sh -c ".venv/bin/uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"
