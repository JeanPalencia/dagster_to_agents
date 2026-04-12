# chat-agent — Claude Code CLI + MCP

Google Chat bot que usa Claude Code CLI para gestionar pipelines de Dagster en Railway.

## Arquitectura

```
Google Chat → FastAPI webhook → claude -p (CLI subprocess) → MCP server (Dagster tools) → Dagster GraphQL
```

**Componentes**:
- `src/chat_agent/main.py` - FastAPI app, recibe webhooks de Google Chat
- `src/chat_agent/agent.py` - Llama `claude -p` con MCP config y system prompt
- `src/dagster_mcp/server.py` - MCP stdio server con 4 tools de Dagster
- `mcp.json` - Config MCP que le dice a Claude dónde encontrar el server

## Variables de entorno (Railway)

| Variable | Valor | Descripción |
|----------|-------|-------------|
| `CLAUDE_CODE_USE_BEDROCK` | `1` | **NUEVA** - Usa AWS Bedrock en vez de API directa |
| `AWS_ACCESS_KEY_ID` | (existente) | Credenciales AWS para Bedrock |
| `AWS_SECRET_ACCESS_KEY` | (existente) | Credenciales AWS |
| `AWS_SESSION_TOKEN` | (existente, si STS) | Token temporal STS |
| `AWS_DEFAULT_REGION` | `us-east-1` | Región de Bedrock |
| `DAGSTER_GRAPHQL_URL` | (existente) | URL GraphQL de Dagster |
| `GOOGLE_CHAT_AUDIENCE` | (existente) | JWT audience para verificación |

**Nota**: `ANTHROPIC_API_KEY` fue removida (nunca se usaba).

## Testing local

### 1. Instalar dependencias

```bash
cd chat-agent
uv sync
npm install -g @anthropic-ai/claude-code
```

### 2. Configurar env vars

```bash
export GOOGLE_CHAT_AUDIENCE=""  # Deshabilita JWT auth para testing
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
export DAGSTER_GRAPHQL_URL=https://dagstertoagents-production.up.railway.app/graphql
```

### 3. Probar el MCP server standalone

```bash
echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | python -m dagster_mcp
```

Deberías ver los 4 tools: `list_jobs`, `launch_job`, `get_run_status`, `get_recent_runs`.

### 4. Arrancar el servidor

```bash
uvicorn chat_agent.main:app --host 0.0.0.0 --port 8000
```

### 5. Probar con curl

```bash
curl -X POST http://localhost:8000/chat/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "chat": {
      "user": {"displayName": "Test User"},
      "messagePayload": {
        "message": {"text": "list jobs"},
        "space": {"name": "spaces/test123"}
      }
    }
  }'
```

### 6. Probar multi-turn (misma session)

```bash
# Primera request
curl -X POST http://localhost:8000/chat/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "chat": {
      "user": {"displayName": "Test"},
      "messagePayload": {
        "message": {"text": "list jobs"},
        "space": {"name": "spaces/test123"}
      }
    }
  }'

# Segunda request (mismo space) - debería tener contexto
curl -X POST http://localhost:8000/chat/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "chat": {
      "user": {"displayName": "Test"},
      "messagePayload": {
        "message": {"text": "launch the first job"},
        "space": {"name": "spaces/test123"}
      }
    }
  }'
```

## Consideraciones de memoria en Railway

Claude Code CLI es un proceso Node.js que consume ~150-200MB adicionales. El subprocess se crea y destruye en cada request (no persiste). Si hay OOM:

1. Upgrade del plan de Railway
2. Reducir `--max-turns` en `agent.py` (actualmente 10)
3. Considerar el approach alternativo (Anthropic SDK sin CLI)

## Timeout de Google Chat

Webhooks de Google Chat tienen timeout de 30 segundos. Si Claude tarda más:
- El webhook fallará con timeout
- El usuario puede reintentar
- Futuro: responder inmediatamente + async response via Google Chat API
