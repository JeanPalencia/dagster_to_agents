FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl tar git && rm -rf /var/lib/apt/lists/*

# Install Engram binary (Linux x86_64)
RUN curl -fsSL https://github.com/Gentleman-Programming/engram/releases/download/v1.12.0/engram_1.12.0_linux_amd64.tar.gz \
    | tar -xz -C /usr/local/bin engram && \
    chmod 755 /usr/local/bin/engram

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

# Copy chat-agent source
COPY . .

# Download dagster-pipeline and .claude from GitHub (sparse, no full clone)
ARG GITHUB_REPO=JeanPalencia/dagster_to_agents
ARG GITHUB_BRANCH=main
RUN git clone --depth 1 --filter=blob:none --sparse \
      https://github.com/${GITHUB_REPO}.git /tmp/repo && \
    cd /tmp/repo && \
    git sparse-checkout set dagster-pipeline .claude && \
    cp -r dagster-pipeline /app/dagster-pipeline && \
    cp -r .claude /app/.claude && \
    rm -rf /tmp/repo
RUN uv sync && chown -R appuser:appuser /app

ENV PATH="/app/.venv/bin:$PATH"

# Switch to non-root for runtime
USER appuser

EXPOSE 8000

CMD ["sh", "-c", "uvicorn chat_agent.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
