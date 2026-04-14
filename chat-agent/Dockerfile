FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl tar git gosu && rm -rf /var/lib/apt/lists/*

# Install Node.js (required for mcp-remote)
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install mcp-remote globally so npx finds it instantly (no download on each call)
RUN npm install -g mcp-remote

# Install GitHub CLI (needed for PR creation by agents) — binary install, no APT keyring needed
RUN curl -fsSL https://github.com/cli/cli/releases/download/v2.65.0/gh_2.65.0_linux_amd64.tar.gz \
    | tar -xz --strip-components=2 -C /usr/local/bin gh_2.65.0_linux_amd64/bin/gh && \
    chmod 755 /usr/local/bin/gh

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

ARG GITHUB_REPO=JeanPalencia/dagster_to_agents
RUN uv sync && chown -R appuser:appuser /app

ENV PATH="/app/.venv/bin:$PATH"
ENV GITHUB_REPO=${GITHUB_REPO}

EXPOSE 8000

# Run as root briefly to:
#   1. Fix Volume permissions
#   2. Clone repo using GITHUB_TOKEN (runtime env var — works for both clone and gh CLI)
#   3. Drop to appuser via gosu
CMD ["sh", "-c", "\
  mkdir -p /data/.engram && chown -R appuser:appuser /data && \
  if [ -n \"$GITHUB_TOKEN\" ]; then \
    git clone --depth 1 https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /app/repo 2>&1 || echo 'WARNING: repo clone failed, logic_modifier will be unavailable'; \
    git -C /app/repo config user.email 'dagster-agent@railway.app' 2>/dev/null || true; \
    git -C /app/repo config user.name 'Dagster Agent' 2>/dev/null || true; \
    chown -R appuser:appuser /app/repo 2>/dev/null || true; \
  else \
    echo 'WARNING: GITHUB_TOKEN not set, skipping repo clone'; \
  fi && \
  exec gosu appuser uvicorn chat_agent.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
