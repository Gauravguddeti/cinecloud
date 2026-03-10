"""
CineCloud — Recommendation Service (GCP Cloud Functions)
=========================================================
Replaces: AWS Lambda + DynamoDB + SQS + API Gateway WebSocket
Uses:      Firestore + Upstash Redis + Cloud Pub/Sub

Entry points:
  http_handler   — HTTP REST API (GET /recommendations, POST /refresh)
  pubsub_handler — Cloud Pub/Sub trigger (async worker, replaces SQS consumer)

Real-time: Worker writes updated recommendations back to Firestore
           → frontend Firestore onSnapshot fires automatically (no WebSocket needed!)

Algorithm: Hybrid CF (70%) + CBF (30%)
  CF  = User-User Collaborative Filtering (cosine similarity on ratings matrix)
  CBF = Content-Based Filtering (TF-IDF weighted genres + cast + keywords)
"""

import base64
import json
import math
import os
import time
from collections import defaultdict
from datetime import datetime

import firebase_admin
from firebase_admin import auth as firebase_auth, firestore

# ── Redis (Upstash) ───────────────────────────────────────────
try:
    import redis as redis_lib
    _redis_available = True
except ImportError:
    _redis_available = False

if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()

REDIS_URL = os.environ.get("REDIS_URL", "")
TOP_N = 20
CF_WEIGHT = 0.70
CBF_WEIGHT = 0.30
MIN_RATINGS_FOR_CF = 3
CACHE_TTL_REDIS = 1800    # 30 min
CACHE_TTL_FIRESTORE = 86400  # 24 h

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}


# ─────────────────────────────────────────────────────────────
#  Cache helpers (Redis + Firestore two-tier)
# ─────────────────────────────────────────────────────────────

def _get_redis():
    if not _redis_available or not REDIS_URL:
        return None
    try:
        r = redis_lib.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as e:
        print(f"[rec] Redis connect failed: {e}")
        return None


def _cache_get(user_id: str) -> list | None:
    # 1. Try Redis
    r = _get_redis()
    if r:
        try:
            cached = r.get(f"rec:{user_id}")
            if cached:
                print(f"[rec] Redis cache HIT for {user_id}")
                return json.loads(cached)
        except Exception as e:
            print(f"[rec] Redis get failed: {e}")

    # 2. Try Firestore cache
    try:
        doc = _db.collection("recommendations").document(user_id).get()
        if doc.exists:
            data = doc.to_dict()
            ttl = int(data.get("ttl", 0))
            if ttl > int(time.time()):
                print(f"[rec] Firestore cache HIT for {user_id}")
                return data.get("recommendations", [])
    except Exception as e:
        print(f"[rec] Firestore cache get failed: {e}")

    return None


def _cache_set(user_id: str, recommendations: list):
    payload = json.dumps(recommendations)

    r = _get_redis()
    if r:
        try:
            r.setex(f"rec:{user_id}", CACHE_TTL_REDIS, payload)
        except Exception as e:
            print(f"[rec] Redis set failed: {e}")

    try:
        _db.collection("recommendations").document(user_id).set({
            "userId": user_id,
            "recommendations": recommendations,
            "computedAt": datetime.utcnow().isoformat(),
            "ttl": int(time.time()) + CACHE_TTL_FIRESTORE,
        })
    except Exception as e:
        print(f"[rec] Firestore cache set failed: {e}")


def _cache_invalidate(user_id: str):
    r = _get_redis()
    if r:
        try:
            r.delete(f"rec:{user_id}")
        except Exception:
            pass
    try:
        _db.collection("recommendations").document(user_id).delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
#  Firestore data loading (replaces DynamoDB scans)
# ─────────────────────────────────────────────────────────────

def _load_all_ratings() -> dict[str, dict[str, float]]:
    """Return {userId: {movieId: rating}} for ALL users from Firestore."""
    ratings: dict[str, dict[str, float]] = defaultdict(dict)
    for doc in _db.collection("ratings").stream():
        r = doc.to_dict()
        ratings[r["userId"]][r["movieId"]] = float(r["rating"])
    return dict(ratings)


def _load_movies_metadata() -> dict[str, dict]:
    """Return {movieId: movie_data} for all movies from Firestore."""
    movies: dict[str, dict] = {}
    for doc in _db.collection("movies").stream():
        data = doc.to_dict()
        movies[data["movieId"]] = data
    return movies


# ─────────────────────────────────────────────────────────────
#  Recommendation Algorithm  ← IDENTICAL to AWS version
# ─────────────────────────────────────────────────────────────

def _cosine_similarity(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    common = set(vec_a) & set(vec_b)
    if not common:
        return 0.0
    dot = sum(vec_a[m] * vec_b[m] for m in common)
    norm_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _collaborative_filter(
    user_id: str,
    all_ratings: dict[str, dict[str, float]],
    top_k_users: int = 15,
) -> dict[str, float]:
    """User-User CF — cosine similarity on ratings matrix."""
    user_ratings = all_ratings.get(user_id, {})
    if len(user_ratings) < MIN_RATINGS_FOR_CF:
        return {}

    similarities = []
    for other_id, other_ratings in all_ratings.items():
        if other_id == user_id:
            continue
        sim = _cosine_similarity(user_ratings, other_ratings)
        if sim > 0:
            similarities.append((sim, other_id))

    similarities.sort(reverse=True)
    top_users = similarities[:top_k_users]
    if not top_users:
        return {}

    scores: dict[str, float] = defaultdict(float)
    sim_sums: dict[str, float] = defaultdict(float)

    for sim, other_id in top_users:
        for movie_id, rating in all_ratings[other_id].items():
            if movie_id not in user_ratings:
                scores[movie_id] += sim * rating
                sim_sums[movie_id] += abs(sim)

    return {mid: scores[mid] / sim_sums[mid] for mid in scores if sim_sums[mid] > 0}


def _build_movie_feature_vector(movie: dict) -> dict[str, float]:
    tf: dict[str, float] = defaultdict(float)
    for genre in movie.get("genres", []):
        tf[f"genre:{genre.lower().replace(' ', '_')}"] += 3.0
    for actor in movie.get("cast", [])[:5]:
        tf[f"cast:{actor.lower().replace(' ', '_')}"] += 2.0
    for kw in movie.get("keywords", [])[:15]:
        tf[f"kw:{kw.lower().replace(' ', '_')}"] += 1.0
    return dict(tf)


def _build_user_preference_vector(
    user_ratings: dict[str, float],
    movies_metadata: dict[str, dict],
) -> dict[str, float]:
    pref: dict[str, float] = defaultdict(float)
    for movie_id, rating in user_ratings.items():
        movie = movies_metadata.get(movie_id)
        if not movie:
            continue
        weight = (rating - 3.0) / 2.0
        for term, tf in _build_movie_feature_vector(movie).items():
            pref[term] += weight * tf
    return dict(pref)


def _content_based_filter(
    user_id: str,
    all_ratings: dict[str, dict[str, float]],
    movies_metadata: dict[str, dict],
) -> dict[str, float]:
    """TF-IDF Content-Based Filtering."""
    user_ratings = all_ratings.get(user_id, {})
    if not user_ratings:
        return {}
    user_pref = _build_user_preference_vector(user_ratings, movies_metadata)
    if not user_pref:
        return {}
    scores: dict[str, float] = {}
    for movie_id, movie in movies_metadata.items():
        if movie_id in user_ratings:
            continue
        sim = _cosine_similarity(user_pref, _build_movie_feature_vector(movie))
        if sim > 0:
            scores[movie_id] = sim
    return scores


def _normalise(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    min_v, max_v = min(scores.values()), max(scores.values())
    spread = max_v - min_v
    if spread == 0:
        return {k: 1.0 for k in scores}
    return {k: (v - min_v) / spread for k, v in scores.items()}


def compute_recommendations(
    user_id: str,
    all_ratings: dict[str, dict[str, float]],
    movies_metadata: dict[str, dict],
    top_n: int = TOP_N,
) -> list[dict]:
    """Hybrid CF+CBF. Falls back to popularity for cold-start users."""
    user_ratings = all_ratings.get(user_id, {})

    if not user_ratings:
        popular = sorted(movies_metadata.values(), key=lambda m: float(m.get("popularity", 0)), reverse=True)
        return _format_recs(popular[:top_n], reason="Popular on CineCloud")

    cf_norm = _normalise(_collaborative_filter(user_id, all_ratings))
    cbf_norm = _normalise(_content_based_filter(user_id, all_ratings, movies_metadata))

    all_candidates = set(cf_norm) | set(cbf_norm)
    if not all_candidates:
        popular = sorted(movies_metadata.values(), key=lambda m: float(m.get("popularity", 0)), reverse=True)
        return _format_recs([m for m in popular if m["movieId"] not in user_ratings][:top_n], reason="Trending")

    hybrid = []
    for mid in all_candidates:
        score = cf_norm.get(mid, 0) * CF_WEIGHT + cbf_norm.get(mid, 0) * CBF_WEIGHT
        reason = (
            "Because users like you enjoyed similar movies"
            if cf_norm.get(mid, 0) > cbf_norm.get(mid, 0)
            else "Because you enjoyed similar genres & cast"
        )
        hybrid.append((score, mid, reason))

    hybrid.sort(reverse=True)
    results = []
    for score, mid, reason in hybrid[:top_n]:
        movie = movies_metadata.get(mid)
        if movie:
            results.append({
                "movieId": mid,
                "title": movie.get("title", ""),
                "posterPath": movie.get("posterPath"),
                "genres": movie.get("genres", []),
                "voteAverage": float(movie.get("voteAverage", 0)),
                "releaseYear": movie.get("releaseYear", ""),
                "score": round(score, 4),
                "reason": reason,
            })
    return results


def _format_recs(movies: list, reason: str) -> list[dict]:
    return [
        {
            "movieId": m.get("movieId", ""),
            "title": m.get("title", ""),
            "posterPath": m.get("posterPath"),
            "genres": m.get("genres", []),
            "voteAverage": float(m.get("voteAverage", 0)),
            "releaseYear": m.get("releaseYear", ""),
            "score": float(m.get("popularity", 0)),
            "reason": reason,
        }
        for m in movies
    ]


# ─────────────────────────────────────────────────────────────
#  HTTP Handler (REST API)
# ─────────────────────────────────────────────────────────────

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
    """Cloud Function HTTP entry point for /recommendations/* routes."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    token_uid = _verify_token(request)
    if not token_uid:
        return _response(401, {"error": "Unauthorized"})

    path = request.path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    user_id = segments[-2] if path.endswith("/refresh") and len(segments) >= 2 else segments[-1]

    if token_uid != user_id:
        return _response(403, {"error": "Forbidden"})

    if path.endswith("/refresh") and request.method == "POST":
        _cache_invalidate(user_id)
        all_ratings = _load_all_ratings()
        movies_metadata = _load_movies_metadata()
        recs = compute_recommendations(user_id, all_ratings, movies_metadata)
        _cache_set(user_id, recs)
        return _response(200, {"recommendations": recs, "userId": user_id, "fromCache": False, "count": len(recs)})

    # GET /recommendations/{userId}
    cached = _cache_get(user_id)
    if cached:
        return _response(200, {"recommendations": cached, "userId": user_id, "fromCache": True, "count": len(cached)})

    t0 = time.time()
    all_ratings = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs = compute_recommendations(user_id, all_ratings, movies_metadata)
    elapsed = round(time.time() - t0, 3)
    _cache_set(user_id, recs)

    return _response(200, {
        "recommendations": recs,
        "userId": user_id,
        "fromCache": False,
        "computeTimeSeconds": elapsed,
        "count": len(recs),
    })


# ─────────────────────────────────────────────────────────────
#  Pub/Sub Worker (replaces SQS consumer Lambda)
#  Triggered by: Cloud Pub/Sub topic 'rating-events'
#  Real-time push: writes updated recs to Firestore → onSnapshot fires on frontend
# ─────────────────────────────────────────────────────────────

def pubsub_handler(event, context):
    """
    Cloud Function Pub/Sub entry point.
    Triggered whenever a rating is submitted.
    Recomputes recommendations and writes to Firestore
    (Firestore onSnapshot on the frontend picks up the change automatically).
    """
    try:
        raw = base64.b64decode(event["data"]).decode("utf-8")
        message = json.loads(raw)
    except Exception as e:
        print(f"[worker] Failed to parse Pub/Sub message: {e}")
        return

    user_id = message.get("userId")
    movie_id = message.get("movieId")
    rating = float(message.get("rating", 0))

    if not user_id or not movie_id:
        print(f"[worker] Skipping malformed message: {message}")
        return

    print(f"[worker] Recomputing recs for user={user_id} after rating movie={movie_id} ({rating}★)")
    t0 = time.time()

    _cache_invalidate(user_id)
    all_ratings = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs = compute_recommendations(user_id, all_ratings, movies_metadata)

    # Write to Firestore — this triggers onSnapshot on the frontend in real-time! ⚡
    _cache_set(user_id, recs)

    print(f"[worker] Done in {round(time.time()-t0, 2)}s — {len(recs)} recs, Firestore updated")
