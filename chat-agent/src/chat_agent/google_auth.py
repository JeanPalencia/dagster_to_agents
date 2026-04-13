"""
Google Chat JWT verification.

Google Chat sends a Bearer token with every webhook request.
We verify it to reject spoofed requests.

Confirmed token format (from logs):
  iss = 'https://accounts.google.com'
  aud = '<webhook_url> '  (trailing space — stripped before comparison)
"""
from __future__ import annotations

import base64
import json
import logging

from fastapi import HTTPException, Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from chat_agent.config import GOOGLE_CHAT_AUDIENCE

logger = logging.getLogger(__name__)

_GOOGLE_ISSUER = "https://accounts.google.com"


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


async def verify_google_chat_token(request: Request) -> None:
    """
    Extracts and verifies the Google Chat Bearer token.
    Raises HTTP 401 if the token is invalid or missing.

    Set GOOGLE_CHAT_AUDIENCE="" to skip verification (local dev only).
    """
    if not GOOGLE_CHAT_AUDIENCE:
        logger.info("Skipping JWT verification (GOOGLE_CHAT_AUDIENCE not set)")
        return

    auth_header = request.headers.get("Authorization", "")
    logger.info("Received Authorization header: %s", auth_header[:50] if auth_header else "MISSING")

    if not auth_header.startswith("Bearer "):
        logger.error("Authorization header malformed or missing: %r", auth_header[:100] if auth_header else None)
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header.removeprefix("Bearer ")

    # Decode payload to get the raw aud (may have trailing space).
    raw = _decode_jwt_payload(token)
    raw_aud = raw.get("aud", "")
    logger.info("JWT raw aud=%r iss=%r", raw_aud, raw.get("iss"))

    # Audience check: strip whitespace before comparing.
    if raw_aud.strip() != GOOGLE_CHAT_AUDIENCE.strip():
        logger.error("Audience mismatch: token=%r expected=%r", raw_aud, GOOGLE_CHAT_AUDIENCE)
        raise HTTPException(status_code=401, detail="Invalid audience")

    # Verify signature using Google's standard OAuth2 certs.
    # Pass the token's raw aud (with space) so verify_token's internal check passes.
    try:
        claims = id_token.verify_token(
            token,
            google_requests.Request(),
            audience=raw_aud,
        )
    except Exception as exc:
        logger.error("JWT signature verification failed: %s", exc)
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

    if claims.get("iss") != _GOOGLE_ISSUER:
        logger.error("Wrong issuer: %r", claims.get("iss"))
        raise HTTPException(status_code=401, detail=f"Unexpected issuer: {claims.get('iss')}")

    logger.info("JWT verified OK")
