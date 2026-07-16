FROM python:3.12-slim

WORKDIR /app

ENV UV_SYSTEM_PYTHON=1

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
COPY src ./src

RUN uv pip install --system -e . "pytest>=9.1.1" "ruff>=0.15.21"

COPY . .

CMD ["python", "-m", "pytest", "-q"]
