"""
Google Chat JWT verification.

Google Chat sends a Bearer token with every webhook request.
We verify it to reject spoofed requests.

Docs: https://developers.google.com/chat/how-tos/authorize-chat-app
"""
from __future__ import annotations

import base64
import json
import logging
import os

from fastapi import HTTPException, Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from chat_agent.config import GOOGLE_CHAT_AUDIENCE

logger = logging.getLogger(__name__)


def _decode_jwt_payload(token: str) -> dict:
    """Decode JWT payload without verifying signature."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        padding = 4 - len(parts[1]) % 4
        payload_bytes = base64.urlsafe_b64decode(parts[1] + "=" * padding)
        return json.loads(payload_bytes.decode())
    except Exception:
        return {}

# Expected audience: the GCP project number
# Google Chat sets aud = project number (as string)
_CHAT_ISSUER = "chat@system.gserviceaccount.com"
# Google Chat tokens are signed with Google's standard OAuth2 keys (not Chat's own SA keys).
# The iss claim identifies the sender as Chat, but key lookup uses the standard certs URL.


async def verify_google_chat_token(request: Request) -> None:
    """
    Extracts and verifies the Google Chat Bearer token.
    Raises HTTP 401 if the token is invalid or missing.

    Set GOOGLE_CHAT_PROJECT_NUMBER="" to skip verification (dev/testing only).
    """
    if not GOOGLE_CHAT_AUDIENCE:
        # Verification disabled — only safe for local testing
        return

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header.removeprefix("Bearer ")

    # Log raw payload for debugging audience mismatch
    raw = _decode_jwt_payload(token)
    logger.info("JWT raw payload: aud=%r iss=%r", raw.get("aud"), raw.get("iss"))
    logger.info("Expected audience: %r", GOOGLE_CHAT_AUDIENCE)

    try:
        claims = id_token.verify_token(
            token,
            google_requests.Request(),
            audience=GOOGLE_CHAT_AUDIENCE,
        )
    except Exception as exc:
        logger.error("JWT verification failed — audience=%s error=%s", GOOGLE_CHAT_AUDIENCE, exc)
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

    if claims.get("iss") != _CHAT_ISSUER:
        logger.error("Wrong issuer — expected=%s got=%s", _CHAT_ISSUER, claims.get("iss"))
        raise HTTPException(
            status_code=401,
            detail=f"Unexpected issuer: {claims.get('iss')}",
        )
    logger.info("JWT verified OK — iss=%s aud=%s", claims.get("iss"), claims.get("aud"))
