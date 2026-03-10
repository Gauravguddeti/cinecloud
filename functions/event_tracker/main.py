"""
CineCloud — Event Tracker (GCP Cloud Functions)
================================================
Replaces: AWS API Gateway WebSocket ($connect / $disconnect / $authorize)

On GCP, real-time updates are handled by Firestore onSnapshot listeners
on the frontend — no WebSocket infra needed.

This function handles:
  POST /events/track  — client-side event logging (page views, clicks)
  GET  /events/health — health check / liveness probe
"""

import json
import os
import time
from datetime import datetime

import firebase_admin
from firebase_admin import auth as firebase_auth, firestore

if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}

ALLOWED_EVENT_TYPES = {
    "page_view", "movie_click", "movie_rating", "search",
    "rec_click", "profile_view", "login", "register",
}


def _response(status_code: int, body: dict):
    return (json.dumps(body), status_code, CORS_HEADERS)


def _verify_token(request) -> str | None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    try:
        return firebase_auth.verify_id_token(header.split("Bearer ")[1])["uid"]
    except Exception:
        return None


def http_handler(request):
    """Cloud Function HTTP entry point for event tracking."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    path = request.path.rstrip("/")

    # ── Health check (no auth required) ──────────────────────
    if path.endswith("/health") or path == "":
        return _response(200, {
            "status": "healthy",
            "service": "CineCloud Event Tracker",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "note": "Real-time updates via Firestore onSnapshot (no WebSocket needed)",
        })

    # ── Track event ──────────────────────────────────────────
    if path.endswith("/track") and request.method == "POST":
        uid = _verify_token(request)
        if not uid:
            return _response(401, {"error": "Unauthorized"})

        body = request.get_json(silent=True) or {}
        event_type = body.get("eventType", "")
        if event_type not in ALLOWED_EVENT_TYPES:
            return _response(400, {"error": f"Invalid event type: {event_type}"})

        event = {
            "userId": uid,
            "eventType": event_type,
            "properties": body.get("properties", {}),
            "sessionId": body.get("sessionId", ""),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "unixTs": int(time.time()),
        }

        try:
            _db.collection("events").add(event)
        except Exception as e:
            print(f"[events] Firestore write failed: {e}")
            # Non-critical — still return success to client
            return _response(200, {"tracked": True, "note": "logged but Firestore write failed"})

        return _response(200, {"tracked": True, "eventType": event_type})

    return _response(404, {"error": "Not found"})
