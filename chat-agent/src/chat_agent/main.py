"""
FastAPI app — Google Chat webhook + health check.

Google Chat event schema (simplified):
{
  "type": "MESSAGE",
  "message": { "text": "run amenity job", "sender": { "displayName": "..." } },
  "space": { "name": "spaces/XXXXX" }
}

Google Chat expects a sync JSON response within 30s:
{ "text": "<response text>" }
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


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post(
    "/chat/webhook",
    dependencies=[Depends(verify_google_chat_token)],
)
async def chat_webhook(request: Request) -> JSONResponse:
    """
    Receives Google Chat events and responds with the agent's reply.

    Handles MESSAGE events. All other event types (ADDED_TO_SPACE,
    REMOVED_FROM_SPACE, CARD_CLICKED) are acknowledged silently.
    """
    body: dict = await request.json()
    event_type: str = body.get("type", "")

    # Silently acknowledge non-message events
    if event_type != "MESSAGE":
        logger.info("Received event type=%s — ignoring", event_type)
        return JSONResponse({"text": ""})

    message = body.get("message", {})
    user_text: str = message.get("text", "").strip()
    sender: str = message.get("sender", {}).get("displayName", "User")

    if not user_text:
        return JSONResponse({"text": "No message received."})

    logger.info("Message from %s: %s", sender, user_text)

    try:
        reply = await run_agent(user_text)
    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        reply = f"An error occurred while processing your request: {exc}"

    return JSONResponse({"text": reply})
