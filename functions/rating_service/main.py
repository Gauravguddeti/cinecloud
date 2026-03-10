"""
CineCloud — Rating Service (GCP Cloud Functions)
=================================================
Replaces: AWS Lambda + DynamoDB + SQS
Uses:      Firestore (ratings collection) + Cloud Pub/Sub

Entry point: http_handler
Deploy:      gcloud functions deploy cinecloud-ratings --entry-point=http_handler ...

Flow: POST /ratings → write to Firestore → publish to Pub/Sub 'rating-events'
      → worker Lambda recomputes recommendations asynchronously
"""

import json
import os
from datetime import datetime

import firebase_admin
from firebase_admin import auth as firebase_auth, firestore
from google.cloud import pubsub_v1

if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()
_publisher = None

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
RATING_EVENTS_TOPIC = "rating-events"

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}


def _response(status_code: int, body: dict):
    return (json.dumps(body, default=str), status_code, CORS_HEADERS)


def _verify_token(request) -> str | None:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    try:
        return firebase_auth.verify_id_token(header.split("Bearer ")[1])["uid"]
    except Exception:
        return None


def _get_publisher():
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


def _publish_rating_event(user_id: str, movie_id: str, rating: float):
    """Publish a rating event to Cloud Pub/Sub for async recommendation recompute."""
    if not GCP_PROJECT_ID:
        print("[rating] GCP_PROJECT_ID not set — skipping Pub/Sub publish")
        return
    try:
        publisher = _get_publisher()
        topic_path = publisher.topic_path(GCP_PROJECT_ID, RATING_EVENTS_TOPIC)
        message = {
            "eventType": "RATING_SUBMITTED",
            "userId": user_id,
            "movieId": movie_id,
            "rating": rating,
            "timestamp": datetime.utcnow().isoformat(),
        }
        publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    except Exception as e:
        # Non-fatal: recommendations will still update on next refresh
        print(f"[rating] Pub/Sub publish failed (non-fatal): {e}")


# ── Handlers ──────────────────────────────────────────────────

def submit_rating(request) -> tuple:
    """POST /ratings — Create or update a rating for a movie."""
    user_id = _verify_token(request)
    if not user_id:
        return _response(401, {"error": "Unauthorized"})

    body = request.get_json(silent=True) or {}
    movie_id = str(body.get("movieId", "")).strip()
    rating_raw = body.get("rating")

    if not movie_id:
        return _response(400, {"error": "movieId is required"})

    try:
        rating = float(rating_raw)
    except (TypeError, ValueError):
        return _response(400, {"error": "rating must be a number between 1 and 5"})

    if not (1 <= rating <= 5):
        return _response(400, {"error": "rating must be between 1 and 5"})

    # Verify movie exists
    movie_doc = _db.collection("movies").document(movie_id).get()
    if not movie_doc.exists:
        return _response(404, {"error": "Movie not found"})
    movie_title = movie_doc.to_dict().get("title", "")

    now = datetime.utcnow().isoformat()
    # Document ID: userId_movieId (ensures one rating per user/movie)
    rating_doc_id = f"{user_id}_{movie_id}"
    rating_ref = _db.collection("ratings").document(rating_doc_id)

    existing = rating_ref.get()
    created_at = existing.to_dict().get("createdAt", now) if existing.exists else now

    rating_ref.set({
        "userId": user_id,
        "movieId": movie_id,
        "rating": rating,
        "title": movie_title,
        "createdAt": created_at,
        "updatedAt": now,
    })

    # Update user total ratings count (only on new ratings)
    if not existing.exists:
        _db.collection("users").document(user_id).update({
            "totalRatings": firestore.Increment(1),
        })

    # Async: trigger recommendation recomputation via Pub/Sub
    _publish_rating_event(user_id, movie_id, rating)

    action = "updated" if existing.exists else "created"
    return _response(200, {
        "message": f"Rating {action} successfully",
        "rating": {"userId": user_id, "movieId": movie_id, "rating": rating},
    })


def get_user_ratings(request, user_id_param: str) -> tuple:
    """GET /ratings/{userId} — Fetch all ratings for the authenticated user."""
    token_user_id = _verify_token(request)
    if not token_user_id:
        return _response(401, {"error": "Unauthorized"})
    if token_user_id != user_id_param:
        return _response(403, {"error": "Forbidden"})

    docs = _db.collection("ratings").where("userId", "==", user_id_param).stream()
    ratings = [d.to_dict() for d in docs]

    return _response(200, {"ratings": ratings, "count": len(ratings)})


# ── Router / Entry Point ──────────────────────────────────────

def http_handler(request):
    """Cloud Function HTTP entry point for /ratings/* routes."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    path = request.path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if request.method == "POST" and path.endswith("/ratings"):
        return submit_rating(request)

    if request.method == "GET" and len(segments) >= 1:
        user_id_param = segments[-1]
        return get_user_ratings(request, user_id_param)

    return _response(404, {"error": "Route not found"})
