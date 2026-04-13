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
      "space": { "name": "spaces/XXXXX" }
    }
  }
}

Response format for Chat add-ons (immediate/sync):
{
  "hostAppDataAction": {
    "chatDataAction": {
      "createMessageAction": { "message": { "text": "..." } }
    }
  }
}

Async responses (for slow tasks) are sent proactively via the Chat REST API
using a service account (GOOGLE_CHAT_SA_EMAIL + GOOGLE_CHAT_SA_PRIVATE_KEY).
"""
from __future__ import annotations

import asyncio
import logging
import os
import pwd

import httpx
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.service_account import Credentials

from chat_agent.agent import run_agent
from chat_agent.google_auth import verify_google_chat_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dagster Chat Agent")

# Log the user the app is running as
try:
    current_user = pwd.getpwuid(os.getuid()).pw_name
    logger.info("🚀 Chat agent starting as user: %s (uid=%d)", current_user, os.getuid())
except Exception as e:
    logger.warning("Could not determine current user: %s", e)

# In-memory session store: {space_name: session_id}
_sessions: dict[str, str] = {}

# Google Chat API timeout before falling back to async response (seconds)
# Keep well under Google Chat's 30s webhook timeout so "⏳ Analizando..."
# arrives before the "no responde" placeholder appears.
_SYNC_TIMEOUT = 5


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


def _get_chat_token() -> str:
    """Get a Google Chat API bearer token using the service account."""
    private_key = os.environ.get("GOOGLE_CHAT_SA_PRIVATE_KEY", "").replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(
        {
            "type": "service_account",
            "client_email": os.environ.get("GOOGLE_CHAT_SA_EMAIL", ""),
            "private_key": private_key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=["https://www.googleapis.com/auth/chat.bot"],
    )
    credentials.refresh(GoogleRequest())
    return credentials.token


async def _send_async_message(space_name: str, text: str) -> None:
    """Send a proactive message to a Google Chat space via REST API."""
    try:
        token = _get_chat_token()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"https://chat.googleapis.com/v1/{space_name}/messages",
                headers={"Authorization": f"Bearer {token}"},
                json={"text": text},
            )
            if resp.status_code != 200:
                logger.error("Failed to send async message: %d %s", resp.status_code, resp.text)
            else:
                logger.info("Async message sent to %s", space_name)
    except Exception as exc:
        logger.exception("Error sending async message: %s", exc)


async def _run_agent_and_reply(
    user_text: str,
    space_name: str,
    session_id: str | None,
) -> None:
    """Background task: run agent and send result via proactive message."""
    try:
        reply, new_session_id = await run_agent(user_text, session_id=session_id)
        if new_session_id and space_name:
            _sessions[space_name] = new_session_id
            logger.info("Stored session %s for space %s", new_session_id, space_name)
    except Exception as exc:
        logger.exception("Agent error in background task: %s", exc)
        reply = f"Error procesando tu mensaje: {exc}"

    if space_name:
        await _send_async_message(space_name, reply)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/debug/engram")
async def debug_engram() -> dict:
    """Check Engram DB status and recent memories."""
    import os, subprocess
    data_dir = os.environ.get("ENGRAM_DATA_DIR", "/data/.engram")
    db_path = f"{data_dir}/engram.db"
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0

    # Query DB directly via sqlite3
    rows = []
    try:
        import sqlite3
        if db_exists:
            con = sqlite3.connect(db_path)
            cur = con.execute(
                "SELECT id, title, type, created_at, substr(content, 1, 150) FROM observations ORDER BY created_at DESC LIMIT 10"
            )
            rows = [{"id": r[0], "title": r[1], "type": r[2], "created_at": r[3], "content_preview": r[4]} for r in cur.fetchall()]
            con.close()
    except Exception as e:
        rows = [{"error": str(e)}]

    # Also check /data permissions and engram binary
    import shutil
    data_writable = os.access("/data", os.W_OK)
    engram_path = shutil.which("engram")

    return {
        "db_exists": db_exists,
        "db_size_bytes": db_size,
        "data_dir": data_dir,
        "data_writable": data_writable,
        "engram_binary": engram_path,
        "recent_memories": rows,
    }


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

    session_id = _sessions.get(space_name) if space_name else None

    # Try to respond synchronously within the timeout.
    # If the agent takes too long, fall back to async response.
    try:
        reply, new_session_id = await asyncio.wait_for(
            run_agent(user_text, session_id=session_id),
            timeout=_SYNC_TIMEOUT,
        )
        if new_session_id and space_name:
            _sessions[space_name] = new_session_id
        return JSONResponse(_chat_response(reply))

    except asyncio.TimeoutError:
        # Agent is taking too long — respond immediately and continue in background
        logger.info("Agent timeout (%ds) — switching to async response", _SYNC_TIMEOUT)
        asyncio.create_task(
            _run_agent_and_reply(user_text, space_name, session_id)
        )
        return JSONResponse(_chat_response("⏳ Analizando tu solicitud, te respondo en un momento..."))

    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        return JSONResponse(_chat_response(f"Error procesando tu mensaje: {exc}"))
