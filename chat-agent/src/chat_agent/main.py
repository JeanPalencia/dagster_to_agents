"""
FastAPI app — Google Chat webhook + health check.

This app uses the Google Workspace Add-ons event format (not classic Chat API).
Actual event structure received:
{
  "commonEventObject": { "hostApp": "CHAT", ... },
  "chat": {
    "user": { "displayName": "...", "email": "..." },
    "eventTime": "...",
    "messagePayload": {
      "message": { "text": "list jobs", ... },
      "space": { ... }
    }
  }
}

Response format for Chat add-ons:
{
  "hostAppDataAction": {
    "chatDataAction": {
      "createMessageAction": { "message": { "text": "..." } }
    }
  }
}
"""
from __future__ import annotations

import logging
import os
import pwd

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from chat_agent.agent import run_agent
from chat_agent.google_auth import verify_google_chat_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dagster Chat Agent")

# Log the user the app is running as (for debugging)
try:
    current_user = pwd.getpwuid(os.getuid()).pw_name
    logger.info("🚀 Chat agent starting as user: %s (uid=%d)", current_user, os.getuid())
except Exception as e:
    logger.warning("Could not determine current user: %s", e)

# In-memory session store: {space_name: session_id}
# Sessions are lost on container restart (acceptable for this use case)
_sessions: dict[str, str] = {}


def _chat_response(text: str) -> dict:
    """Build a Google Workspace Add-on Chat response."""
    return {
        "hostAppDataAction": {
            "chatDataAction": {
                "createMessageAction": {
                    "message": {"text": text}
                }
            }
        }
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/debug/sdk-info")
async def debug_sdk_info() -> dict:
    """Inspect ClaudeAgentOptions fields and test SDK."""
    import asyncio
    import inspect
    import os
    from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query

    # 1. Show ClaudeAgentOptions signature
    sig = str(inspect.signature(ClaudeAgentOptions.__init__))

    # 2. Try a minimal SDK call (no MCP, no tools) with 10s timeout
    result = None
    try:
        options = ClaudeAgentOptions(
            env={
                "CLAUDE_CODE_USE_BEDROCK": "1",
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
                "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            },
            max_turns=1,
        )
        messages = []
        async def _run():
            async for msg in query(prompt="say hello", options=options):
                messages.append({"type": type(msg).__name__, "repr": repr(msg)[:200]})
        await asyncio.wait_for(_run(), timeout=15)
        result = {"status": "ok", "messages": messages}
    except asyncio.TimeoutError:
        result = {"status": "timeout_15s", "messages_so_far": [{"type": type(m).__name__} for m in messages]}
    except Exception as e:
        result = {"status": "error", "error": str(e)}

    return {"signature": sig, "sdk_test": result}


@app.post(
    "/chat/webhook",
    dependencies=[Depends(verify_google_chat_token)],
)
async def chat_webhook(request: Request) -> JSONResponse:
    body: dict = await request.json()

    chat_event = body.get("chat", {})
    message_payload = chat_event.get("messagePayload", {})
    message = message_payload.get("message", {})
    user_text: str = message.get("text", "").strip()

    if not user_text:
        logger.info("No message text in payload — ignoring")
        return JSONResponse({})

    sender: str = chat_event.get("user", {}).get("displayName", "User")
    space_name: str = message_payload.get("space", {}).get("name", "")
    logger.info("Message from %s in space %s: %s", sender, space_name, user_text)

    # Look up existing session for this space
    session_id = _sessions.get(space_name) if space_name else None

    try:
        reply, new_session_id = await run_agent(user_text, session_id=session_id)

        # Store the new session ID if we have one
        if new_session_id and space_name:
            _sessions[space_name] = new_session_id
            logger.info("Stored session %s for space %s", new_session_id, space_name)
    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        reply = f"Error procesando tu mensaje: {exc}"

    return JSONResponse(_chat_response(reply))
