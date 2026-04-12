FROM python:3.12-slim

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install UV and make it executable by all users
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv && \
    cp /root/.local/bin/uvx /usr/local/bin/uvx && \
    chmod 755 /usr/local/bin/uv /usr/local/bin/uvx

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app && \
    chown -R appuser:appuser /app

WORKDIR /app

# Copy and install as root, then chown
COPY . .
RUN uv sync && chown -R appuser:appuser /app

ENV PATH="/app/.venv/bin:$PATH"

# Switch to non-root for runtime
USER appuser

EXPOSE 8000

CMD ["sh", "-c", "uvicorn chat_agent.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
