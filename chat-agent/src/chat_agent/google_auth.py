"""
Google Chat JWT verification.

Google Chat sends a Bearer token with every webhook request.
We verify it to reject spoofed requests.

Docs: https://developers.google.com/chat/how-tos/authorize-chat-app
"""
from __future__ import annotations

import os

from fastapi import HTTPException, Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from chat_agent.config import GOOGLE_CHAT_PROJECT_NUMBER

# Expected audience: the GCP project number
# Google Chat sets aud = project number (as string)
_CHAT_ISSUER = "chat@system.gserviceaccount.com"


async def verify_google_chat_token(request: Request) -> None:
    """
    Extracts and verifies the Google Chat Bearer token.
    Raises HTTP 401 if the token is invalid or missing.

    Set GOOGLE_CHAT_PROJECT_NUMBER="" to skip verification (dev/testing only).
    """
    if not GOOGLE_CHAT_PROJECT_NUMBER:
        # Verification disabled — only safe for local testing
        return

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header.removeprefix("Bearer ")

    try:
        claims = id_token.verify_token(
            token,
            google_requests.Request(),
            audience=GOOGLE_CHAT_PROJECT_NUMBER,
        )
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

    if claims.get("iss") != _CHAT_ISSUER:
        raise HTTPException(
            status_code=401,
            detail=f"Unexpected issuer: {claims.get('iss')}",
        )
