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

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from chat_agent.agent import run_agent
from chat_agent.google_auth import verify_google_chat_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dagster Chat Agent")


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
    logger.info("Message from %s: %s", sender, user_text)

    try:
        reply = await run_agent(user_text)
    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        reply = f"Error procesando tu mensaje: {exc}"

    return JSONResponse(_chat_response(reply))
