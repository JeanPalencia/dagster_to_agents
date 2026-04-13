FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl tar git gosu && rm -rf /var/lib/apt/lists/*

# Install Node.js (required for mcp-remote)
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install mcp-remote globally so npx finds it instantly (no download on each call)
RUN npm install -g mcp-remote

# Install Engram binary (Linux x86_64)
RUN curl -fsSL https://github.com/Gentleman-Programming/engram/releases/download/v1.12.0/engram_1.12.0_linux_amd64.tar.gz \
    | tar -xz -C /usr/local/bin engram && \
    chmod 755 /usr/local/bin/engram

# Install UV and make it executable by all users
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv && \
    cp /root/.local/bin/uvx /usr/local/bin/uvx && \
    chmod 755 /usr/local/bin/uv /usr/local/bin/uvx

# Create non-root user and volume mount point with correct permissions
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app /data && \
    chown -R appuser:appuser /app /data

WORKDIR /app

# Copy chat-agent source
COPY . .

# Clone full repo so logic_modifier can commit and push changes
ARG GITHUB_REPO=JeanPalencia/dagster_to_agents
ARG GITHUB_BRANCH=main
ARG GITHUB_TOKEN
RUN git clone --depth 1 \
      https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /app/repo && \
    git -C /app/repo config user.email "dagster-agent@railway.app" && \
    git -C /app/repo config user.name "Dagster Agent"
RUN uv sync && chown -R appuser:appuser /app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# Run as root briefly to fix Volume permissions, then drop to appuser
CMD ["sh", "-c", "mkdir -p /data/.engram && chown -R appuser:appuser /data && exec gosu appuser uvicorn chat_agent.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
