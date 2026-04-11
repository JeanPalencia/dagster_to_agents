FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY dagster-pipeline/ ./dagster-pipeline/

WORKDIR /app/dagster-pipeline

# Install project + dev deps (includes dagster-webserver)
RUN uv sync --group dev

ENV DAGSTER_HOME=/app/dagster-pipeline
ENV PATH="/app/dagster-pipeline/.venv/bin:$PATH"

EXPOSE 3000

COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
