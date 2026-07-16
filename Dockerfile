FROM python:3.12-slim

WORKDIR /app

ENV UV_SYSTEM_PYTHON=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends make \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

RUN uv pip install --system -e ".[dev]"

COPY . .

CMD ["make", "test"]