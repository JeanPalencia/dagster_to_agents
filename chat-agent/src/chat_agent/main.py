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
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
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


async def _send_async_message(space_name: str, text: str) -> str | None:
    """
    Send a proactive message to a Google Chat space via REST API.

    Returns:
        message_name (e.g., "spaces/.../messages/...") or None if failed
    """
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
                return None
            else:
                result = resp.json()
                message_name = result.get("name")  # "spaces/.../messages/..."
                logger.info("Async message sent to %s: %s", space_name, message_name)
                return message_name
    except Exception as exc:
        logger.exception("Error sending async message: %s", exc)
        return None


async def _update_message(message_name: str, new_text: str) -> bool:
    """
    Update an existing Google Chat message.

    Args:
        message_name: Full message name (spaces/.../messages/...)
        new_text: New text content

    Returns:
        True if successful, False otherwise
    """
    try:
        token = _get_chat_token()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.patch(
                f"https://chat.googleapis.com/v1/{message_name}",
                headers={"Authorization": f"Bearer {token}"},
                json={"text": new_text},
                params={"updateMask": "text"},
            )
            if resp.status_code != 200:
                logger.warning("Failed to update message %s: %d %s", message_name, resp.status_code, resp.text)
                return False
            logger.debug("Updated message %s", message_name)
            return True
    except Exception as exc:
        logger.warning("Error updating message %s: %s", message_name, exc)
        return False


async def _run_agent_and_reply(
    user_text: str,
    space_name: str,
    session_id: str | None,
) -> None:
    """Background task: run agent with live progress updates and send result."""
    from chat_agent.progress_tracker import get_initial_message, infer_status_from_event

    # Send initial progress message
    initial_msg = get_initial_message()
    message_name = await _send_async_message(space_name, initial_msg)

    if not message_name:
        logger.error("Failed to send initial message, cannot track progress")
        return

    # Progress callback to update the message
    last_status = initial_msg
    async def progress_callback(event: Any) -> None:
        nonlocal last_status
        new_status = infer_status_from_event(event)
        if new_status and new_status != last_status:
            last_status = new_status
            await _update_message(message_name, new_status)

    # Run agent with progress tracking
    try:
        reply, new_session_id = await run_agent(
            user_text,
            session_id=session_id,
            progress_callback=progress_callback,
        )
        if new_session_id and space_name:
            _sessions[space_name] = new_session_id
            logger.info("Stored session %s for space %s", new_session_id, space_name)
    except Exception as exc:
        logger.exception("Agent error in background task: %s", exc)
        reply = f"❌ Error procesando tu mensaje: {exc}"

    # Update with final result
    await _update_message(message_name, reply)


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


def _verify_agent_secret(request: Request) -> None:
    """Verify the shared secret for agent-to-agent communication."""
    auth_header = request.headers.get("Authorization", "")
    expected_secret = os.environ.get("AGENT_SECRET", "")

    if not expected_secret:
        logger.warning("AGENT_SECRET not configured - agent endpoints disabled")
        raise HTTPException(status_code=503, detail="Agent endpoints not configured")

    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = auth_header.split("Bearer ", 1)[1]
    if token != expected_secret:
        logger.warning("Invalid AGENT_SECRET provided")
        raise HTTPException(status_code=403, detail="Invalid secret")


@app.post("/agent/test")
async def agent_test(request: Request) -> JSONResponse:
    """
    Test specialist endpoint - validates PR changes against real data.

    Called by GitHub Actions when a PR is opened or synchronized.
    """
    _verify_agent_secret(request)

    body: dict = await request.json()
    pr_number = body.get("pr_number")
    branch = body.get("branch")
    flow_name = body.get("flow_name")
    changed_files = body.get("changed_files", [])

    if not pr_number or not branch or not flow_name:
        return JSONResponse(
            {"error": "Missing required fields: pr_number, branch, flow_name"},
            status_code=400,
        )

    logger.info("Test request for PR #%s (branch: %s, flow: %s)", pr_number, branch, flow_name)

    # Build prompt for test_specialist
    prompt = f"""Test PR #{pr_number} for the {flow_name} flow.

Branch: {branch}
Changed files: {', '.join(changed_files) if changed_files else 'unknown'}

Tasks:
1. Read the PR diff via gh CLI
2. Understand what changed
3. Run the test harness: uv run python -m tests.test_harness --flow={flow_name}
4. Design flow-specific assertions if needed
5. Post test results as PR comment
6. Add label: tests/passed or tests/failed

Use the test_specialist sub-agent (you are already in worktree isolation on the PR branch).
"""

    # Run test_specialist agent (this will take time - GitHub Actions will poll)
    try:
        result_text, _ = await run_agent(prompt, session_id=None)
        logger.info("Test specialist completed for PR #%s", pr_number)

        return JSONResponse({
            "status": "completed",
            "pr_number": pr_number,
            "message": "Test specialist executed",
            "result_preview": result_text[:500] if result_text else "No output",
        })

    except Exception as exc:
        logger.exception("Error running test specialist: %s", exc)
        return JSONResponse(
            {
                "status": "error",
                "pr_number": pr_number,
                "error": str(exc),
            },
            status_code=500,
        )


@app.post("/agent/review")
async def agent_review(request: Request) -> JSONResponse:
    """
    Reviewer endpoint - exhaustive PR review against ARCHITECTURE.md.

    Called by GitHub Actions after test specialist passes.
    Posts a structured GitHub review (APPROVE or REQUEST_CHANGES) with confidence level.
    """
    _verify_agent_secret(request)

    body: dict = await request.json()
    pr_number = body.get("pr_number")
    flow_name = body.get("flow_name")
    test_results = body.get("test_results", "not available")

    if not pr_number or not flow_name:
        return JSONResponse(
            {"error": "Missing required fields: pr_number, flow_name"},
            status_code=400,
        )

    logger.info("Review request for PR #%s (flow: %s)", pr_number, flow_name)

    prompt = f"""Review PR #{pr_number} for the {flow_name} flow.

Test specialist results:
{test_results}

Tasks:
1. Read the full PR diff via gh CLI
2. Read dagster-pipeline/ARCHITECTURE.md (all 12 design rules)
3. Read the modified files completely
4. Evaluate compliance against each of the 12 rules
5. Assess logic correctness and edge cases
6. Use spot2 MCP to query gold output data if needed to validate business logic
7. Determine confidence (HIGH/MEDIUM/LOW) and decision (APPROVE/REQUEST_CHANGES)
8. Post review to GitHub via gh api
9. Add appropriate labels (review/approved or review/changes-requested, confidence/*)

Use the reviewer sub-agent.
"""

    try:
        result_text, _ = await run_agent(prompt, session_id=None)
        logger.info("Reviewer completed for PR #%s", pr_number)

        return JSONResponse({
            "status": "completed",
            "pr_number": pr_number,
            "message": "Reviewer executed",
            "result_preview": result_text[:500] if result_text else "No output",
        })

    except Exception as exc:
        logger.exception("Error running reviewer: %s", exc)
        return JSONResponse(
            {"status": "error", "pr_number": pr_number, "error": str(exc)},
            status_code=500,
        )


@app.post("/agent/iterate")
async def agent_iterate(request: Request) -> JSONResponse:
    """
    Iteration endpoint - triggers logic_modifier to fix issues on an existing PR.

    Called by GitHub Actions when tests fail or reviewer rejects.
    """
    _verify_agent_secret(request)

    body: dict = await request.json()
    pr_number = body.get("pr_number")
    branch = body.get("branch")
    feedback = body.get("feedback", "")
    iteration = body.get("iteration", 2)

    if not pr_number or not branch:
        return JSONResponse(
            {"error": "Missing required fields: pr_number, branch"},
            status_code=400,
        )

    logger.info("Iterate request for PR #%s (iteration %s)", pr_number, iteration)

    prompt = f"""Fix the issues on PR #{pr_number} (iteration {iteration}/3).

Branch: {branch}
Feedback from previous attempt:
{feedback}

Tasks:
1. Read the feedback above carefully
2. Check out branch {branch} (already in worktree isolation)
3. Fix the specific issues mentioned in the feedback
4. Run the test harness again to verify fixes
5. Push new commits to the same branch (do NOT create a new PR)
6. Post a comment on PR #{pr_number} explaining what was fixed

Use the logic_modifier sub-agent with the iteration context.
"""

    try:
        result_text, _ = await run_agent(prompt, session_id=None)
        logger.info("Iteration %s completed for PR #%s", iteration, pr_number)

        return JSONResponse({
            "status": "completed",
            "pr_number": pr_number,
            "iteration": iteration,
            "message": "Iteration executed",
            "result_preview": result_text[:500] if result_text else "No output",
        })

    except Exception as exc:
        logger.exception("Error running iteration: %s", exc)
        return JSONResponse(
            {"status": "error", "pr_number": pr_number, "error": str(exc)},
            status_code=500,
        )


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
        # Agent is taking too long — let the background task send and manage the single message
        logger.info("Agent timeout (%ds) — switching to async response", _SYNC_TIMEOUT)
        asyncio.create_task(
            _run_agent_and_reply(user_text, space_name, session_id)
        )
        # Return empty response so Google Chat doesn't create a second message.
        # _run_agent_and_reply sends its own first message and edits it with progress.
        return JSONResponse({})

    except Exception as exc:
        logger.exception("Agent error: %s", exc)
        return JSONResponse(_chat_response(f"Error procesando tu mensaje: {exc}"))
