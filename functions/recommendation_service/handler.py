"""
Hybrid Recommendation Algorithm
================================
GET  /recommendations/{userId}         — Return cached or freshly computed recommendations
POST /recommendations/{userId}/refresh — Force recomputation and cache invalidation

Algorithm:
  score(movie) = 0.70 × CF_score + 0.30 × CBF_score

  Collaborative Filtering (CF):
    User-User CF with cosine similarity on the ratings matrix.
    "Users who rated the same movies similarly also liked X."

  Content-Based Filtering (CBF):
    TF-IDF vector representation of each movie's genres + cast + keywords.
    Cosine similarity between a user's preference vector and each movie.

Cache Strategy (two-tier):
  1. Redis (Upstash) — fastest, TTL 30 min
  2. DynamoDB RecommendationsTable — fallback, TTL 24 h
"""

import json
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

# Optional Redis import (Upstash is redis-compatible)
try:
    import redis as redis_lib
    _redis_available = True
except ImportError:
    _redis_available = False

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))

RATINGS_TABLE = os.environ["RATINGS_TABLE"]
MOVIES_TABLE = os.environ["MOVIES_TABLE"]
RECOMMENDATIONS_TABLE = os.environ["RECOMMENDATIONS_TABLE"]
REDIS_URL = os.environ.get("REDIS_URL", "")

# Recommendation parameters
TOP_N = 20                           # recommendations to return
CF_WEIGHT = 0.70                     # collaborative filtering weight
CBF_WEIGHT = 0.30                    # content-based filtering weight
CACHE_TTL_REDIS = 1800               # 30 minutes in Redis
CACHE_TTL_DYNAMO = 86400             # 24 hours in DynamoDB
MIN_RATINGS_FOR_CF = 3               # minimum ratings before CF is used


# ─────────────────────────────────────────────────────────────
#  Cache helpers
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
    """Try Redis first, then DynamoDB."""
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

    # 2. Try DynamoDB
    try:
        table = dynamodb.Table(RECOMMENDATIONS_TABLE)
        result = table.get_item(Key={"userId": user_id})
        item = result.get("Item")
        if item:
            ttl = int(item.get("ttl", 0))
            if ttl > int(time.time()):
                print(f"[rec] DynamoDB cache HIT for {user_id}")
                return item.get("recommendations", [])
    except Exception as e:
        print(f"[rec] DynamoDB cache get failed: {e}")

    return None


def _cache_set(user_id: str, recommendations: list):
    """Write to both Redis and DynamoDB."""
    payload = json.dumps(recommendations)

    # Redis
    r = _get_redis()
    if r:
        try:
            r.setex(f"rec:{user_id}", CACHE_TTL_REDIS, payload)
        except Exception as e:
            print(f"[rec] Redis set failed: {e}")

    # DynamoDB
    try:
        table = dynamodb.Table(RECOMMENDATIONS_TABLE)
        table.put_item(
            Item={
                "userId": user_id,
                "recommendations": recommendations,
                "computedAt": datetime.utcnow().isoformat(),
                "ttl": int(time.time()) + CACHE_TTL_DYNAMO,
            }
        )
    except Exception as e:
        print(f"[rec] DynamoDB cache set failed: {e}")


def _cache_invalidate(user_id: str):
    """Remove stale cache entries."""
    r = _get_redis()
    if r:
        try:
            r.delete(f"rec:{user_id}")
        except Exception:
            pass

    try:
        table = dynamodb.Table(RECOMMENDATIONS_TABLE)
        table.delete_item(Key={"userId": user_id})
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
#  Data loading
# ─────────────────────────────────────────────────────────────

def _load_all_ratings() -> dict[str, dict[str, float]]:
    """Return {userId: {movieId: rating}} for ALL users."""
    table = dynamodb.Table(RATINGS_TABLE)
    ratings: dict[str, dict[str, float]] = defaultdict(dict)

    result = table.scan(ProjectionExpression="userId, movieId, rating")
    for item in result.get("Items", []):
        ratings[item["userId"]][item["movieId"]] = float(item["rating"])

    # Handle DynamoDB pagination
    while "LastEvaluatedKey" in result:
        result = table.scan(
            ProjectionExpression="userId, movieId, rating",
            ExclusiveStartKey=result["LastEvaluatedKey"],
        )
        for item in result.get("Items", []):
            ratings[item["userId"]][item["movieId"]] = float(item["rating"])

    return dict(ratings)


def _load_movies_metadata() -> dict[str, dict]:
    """Return {movieId: movie_item} for all movies."""
    table = dynamodb.Table(MOVIES_TABLE)
    movies: dict[str, dict] = {}

    result = table.scan(
        ProjectionExpression="movieId, title, genres, #cast, keywords, posterPath, popularity, voteAverage, releaseYear",
        ExpressionAttributeNames={"#cast": "cast"},
    )
    for item in result.get("Items", []):
        movies[item["movieId"]] = item

    while "LastEvaluatedKey" in result:
        result = table.scan(
            ProjectionExpression="movieId, title, genres, #cast, keywords, posterPath, popularity, voteAverage, releaseYear",
            ExpressionAttributeNames={"#cast": "cast"},
            ExclusiveStartKey=result["LastEvaluatedKey"],
        )
        for item in result.get("Items", []):
            movies[item["movieId"]] = item

    return movies


# ─────────────────────────────────────────────────────────────
#  Collaborative Filtering — cosine similarity
# ─────────────────────────────────────────────────────────────

def _cosine_similarity(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    """Cosine similarity between two rating dictionaries."""
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
    """
    User-User Collaborative Filtering.
    Returns {movieId: predicted_score} for unrated movies.
    """
    user_ratings = all_ratings.get(user_id, {})
    if len(user_ratings) < MIN_RATINGS_FOR_CF:
        return {}

    # Compute similarity with every other user
    similarities: list[tuple[float, str]] = []
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

    # Weighted sum of ratings from similar users for unrated movies
    scores: dict[str, float] = defaultdict(float)
    sim_sums: dict[str, float] = defaultdict(float)

    for sim, other_id in top_users:
        for movie_id, rating in all_ratings[other_id].items():
            if movie_id not in user_ratings:          # only recommend unrated movies
                scores[movie_id] += sim * rating
                sim_sums[movie_id] += abs(sim)

    # Normalise
    return {
        mid: scores[mid] / sim_sums[mid]
        for mid in scores
        if sim_sums[mid] > 0
    }


# ─────────────────────────────────────────────────────────────
#  Content-Based Filtering — TF-IDF + cosine similarity
# ─────────────────────────────────────────────────────────────

def _build_movie_feature_vector(movie: dict) -> dict[str, float]:
    """Build a term-frequency vector for a movie (genres × 3, cast × 2, keywords × 1)."""
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
    """Aggregate movie feature vectors weighted by user ratings."""
    pref: dict[str, float] = defaultdict(float)

    for movie_id, rating in user_ratings.items():
        movie = movies_metadata.get(movie_id)
        if not movie:
            continue
        weight = (rating - 3.0) / 2.0   # normalise: poor = -1, great = +1
        for term, tf in _build_movie_feature_vector(movie).items():
            pref[term] += weight * tf

    return dict(pref)


def _content_based_filter(
    user_id: str,
    all_ratings: dict[str, dict[str, float]],
    movies_metadata: dict[str, dict],
) -> dict[str, float]:
    """
    Content-Based Filtering using TF-IDF cosine similarity.
    Returns {movieId: similarity_score} for unrated movies.
    """
    user_ratings = all_ratings.get(user_id, {})
    if not user_ratings:
        return {}

    user_pref = _build_user_preference_vector(user_ratings, movies_metadata)
    if not user_pref:
        return {}

    scores: dict[str, float] = {}

    for movie_id, movie in movies_metadata.items():
        if movie_id in user_ratings:
            continue  # skip already-rated movies

        movie_vec = _build_movie_feature_vector(movie)
        sim = _cosine_similarity(user_pref, movie_vec)
        if sim > 0:
            scores[movie_id] = sim

    return scores


# ─────────────────────────────────────────────────────────────
#  Hybrid combiner
# ─────────────────────────────────────────────────────────────

def _normalise(scores: dict[str, float]) -> dict[str, float]:
    """Min-max normalise a score dictionary to [0, 1]."""
    if not scores:
        return {}
    min_v = min(scores.values())
    max_v = max(scores.values())
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
    """
    Hybrid CF + CBF recommendation.
    Falls back to popularity-based if user has no ratings.
    """
    user_ratings = all_ratings.get(user_id, {})

    # Cold start: user has no ratings → return top popular movies
    if not user_ratings:
        popular = sorted(
            movies_metadata.values(),
            key=lambda m: float(m.get("popularity", 0)),
            reverse=True,
        )[:top_n]
        return _format_recommendations(popular, movies_metadata, reason="Popular on CineCloud")

    # 1. Compute CF scores
    cf_raw = _collaborative_filter(user_id, all_ratings)
    cf_norm = _normalise(cf_raw)

    # 2. Compute CBF scores
    cbf_raw = _content_based_filter(user_id, all_ratings, movies_metadata)
    cbf_norm = _normalise(cbf_raw)

    # 3. Hybrid combination
    all_candidates = set(cf_norm) | set(cbf_norm)
    if not all_candidates:
        # Fallback: content-only from genres of rated movies
        popular = sorted(movies_metadata.values(), key=lambda m: float(m.get("popularity", 0)), reverse=True)
        return _format_recommendations(
            [m for m in popular if m["movieId"] not in user_ratings][:top_n],
            movies_metadata,
            reason="Trending",
        )

    hybrid: list[tuple[float, str]] = []
    for mid in all_candidates:
        cf_s = cf_norm.get(mid, 0) * CF_WEIGHT
        cbf_s = cbf_norm.get(mid, 0) * CBF_WEIGHT
        total = cf_s + cbf_s

        # Reason tag for the UI
        if cf_norm.get(mid, 0) > cbf_norm.get(mid, 0):
            reason = "Because users like you enjoyed similar movies"
        else:
            reason = "Because you enjoyed similar genres & cast"

        hybrid.append((total, mid, reason))

    hybrid.sort(reverse=True)
    top_movies = hybrid[:top_n]

    results = []
    for score, mid, reason in top_movies:
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


def _format_recommendations(movies: list, movies_metadata: dict, reason: str = "") -> list[dict]:
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
#  HTTP Handlers
# ─────────────────────────────────────────────────────────────

def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body),
    }


def get_recommendations(event: dict) -> dict:
    """GET /recommendations/{userId}"""
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    token_user_id = claims.get("sub")
    path_user_id = (event.get("pathParameters") or {}).get("userId")

    if token_user_id != path_user_id:
        return _response(403, {"error": "Forbidden"})

    # Check cache first
    cached = _cache_get(path_user_id)
    if cached:
        return _response(200, {
            "recommendations": cached,
            "userId": path_user_id,
            "fromCache": True,
            "count": len(cached),
        })

    # Cache miss → compute
    t0 = time.time()
    all_ratings = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs = compute_recommendations(path_user_id, all_ratings, movies_metadata)
    elapsed = round(time.time() - t0, 3)

    _cache_set(path_user_id, recs)

    return _response(200, {
        "recommendations": recs,
        "userId": path_user_id,
        "fromCache": False,
        "computeTimeSeconds": elapsed,
        "count": len(recs),
    })


def refresh_recommendations(event: dict) -> dict:
    """POST /recommendations/{userId}/refresh — Force recomputation."""
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    token_user_id = claims.get("sub")
    path_user_id = (event.get("pathParameters") or {}).get("userId")

    if token_user_id != path_user_id:
        return _response(403, {"error": "Forbidden"})

    _cache_invalidate(path_user_id)

    all_ratings = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs = compute_recommendations(path_user_id, all_ratings, movies_metadata)
    _cache_set(path_user_id, recs)

    return _response(200, {
        "recommendations": recs,
        "userId": path_user_id,
        "fromCache": False,
        "count": len(recs),
    })


def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return _response(200, {})

    if method == "GET" and "/recommendations/" in path:
        return get_recommendations(event)

    if method == "POST" and path.endswith("/refresh"):
        return refresh_recommendations(event)

    return _response(404, {"error": "Route not found"})
