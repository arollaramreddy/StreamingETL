FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# I install Java because local PySpark needs a JVM inside the container.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# I install uv once, then use uv.lock so container dependencies match local ones.
RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project

COPY src ./src
COPY main.py ./main.py

CMD ["python", "main.py"]
