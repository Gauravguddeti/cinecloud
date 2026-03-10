"""
CineCloud — ETL Pipeline (GCP Cloud Functions)
================================================
Replaces: AWS Lambda + DynamoDB BatchWriteItem
Uses:      TMDB API → Firestore (movies collection)

Entry point: http_handler
Deploy:      gcloud functions deploy cinecloud-ingest --entry-point=http_handler ...
Also scheduled via Cloud Scheduler: every Sunday 2 AM UTC
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime

import firebase_admin
from firebase_admin import firestore

if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG = "https://image.tmdb.org/t/p/w500"
TMDB_BACKDROP = "https://image.tmdb.org/t/p/w1280"

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
}


def _response(status_code: int, body: dict):
    return (json.dumps(body), status_code, CORS_HEADERS)


def _tmdb_get(path: str, params: dict = None) -> dict:
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    qs = urllib.parse.urlencode(params)
    url = f"{TMDB_BASE}{path}?{qs}"
    with urllib.request.urlopen(url, timeout=10) as resp:  # nosec — trusted TMDB API
        return json.loads(resp.read().decode())


def _fetch_genre_map() -> dict:
    data = _tmdb_get("/genre/movie/list")
    return {g["id"]: g["name"] for g in data.get("genres", [])}


def _transform_movie(raw: dict, genre_map: dict, details: dict = None) -> dict:
    genre_names = [genre_map.get(gid, "") for gid in raw.get("genre_ids", []) if genre_map.get(gid)]

    cast = []
    keywords = []
    if details:
        cast = [m["name"] for m in details.get("credits", {}).get("cast", [])[:10]]
        keywords = [k["name"] for k in details.get("keywords", {}).get("keywords", [])[:20]]

    title = raw.get("title", "")
    overview = raw.get("overview", "")
    release_year = (raw.get("release_date") or "")[:4]

    return {
        "movieId": str(raw["id"]),
        "title": title,
        "titleLower": title.lower(),
        "overviewLower": overview.lower(),
        "castSearch": " ".join(cast).lower(),
        "overview": overview,
        "releaseYear": release_year,
        "genres": genre_names,
        "genre": genre_names[0] if genre_names else "Unknown",
        "cast": cast,
        "keywords": keywords,
        "posterPath": f"{TMDB_IMG}{raw['poster_path']}" if raw.get("poster_path") else None,
        "backdropPath": f"{TMDB_BACKDROP}{raw['backdrop_path']}" if raw.get("backdrop_path") else None,
        "popularity": float(round(raw.get("popularity", 0), 4)),
        "voteAverage": float(round(raw.get("vote_average", 0), 2)),
        "voteCount": int(raw.get("vote_count", 0)),
        "language": raw.get("original_language", "en"),
        "tmdbId": int(raw["id"]),
        "ingestedAt": datetime.utcnow().isoformat(),
    }


def _firestore_batch_write(items: list):
    """Write items to Firestore in batches of 500 (Firestore limit)."""
    for i in range(0, len(items), 500):
        batch = _db.batch()
        for item in items[i:i + 500]:
            ref = _db.collection("movies").document(item["movieId"])
            batch.set(ref, item)
        batch.commit()


def ingest(pages: int = 10) -> dict:
    if not TMDB_API_KEY:
        return {"error": "TMDB_API_KEY not configured"}

    genre_map = _fetch_genre_map()
    total_ingested = 0
    errors = []

    for page in range(1, pages + 1):
        try:
            data = _tmdb_get("/movie/popular", {"page": page, "language": "en-US"})
            items = []

            for raw in data.get("results", []):
                try:
                    details = None
                    if page <= 5:   # fetch credits for first 5 pages
                        try:
                            details = _tmdb_get(
                                f"/movie/{raw['id']}",
                                {"append_to_response": "credits,keywords"},
                            )
                        except Exception:
                            pass
                    items.append(_transform_movie(raw, genre_map, details))
                except Exception as e:
                    errors.append({"movieId": str(raw.get("id")), "error": str(e)})

            _firestore_batch_write(items)
            total_ingested += len(items)
            print(f"[ETL] page {page}/{pages} — ingested {len(items)} movies (total: {total_ingested})")
            time.sleep(0.3)  # Respect TMDB rate limit

        except Exception as e:
            errors.append({"page": page, "error": str(e)})
            print(f"[ETL] error on page {page}: {e}")

    return {
        "totalIngested": total_ingested,
        "pages": pages,
        "errors": errors[:10],
        "completedAt": datetime.utcnow().isoformat(),
    }


def http_handler(request):
    """Cloud Function HTTP entry point — triggers ETL pipeline."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    if request.method != "POST":
        return _response(405, {"error": "Method not allowed"})

    pages = 10
    try:
        body = request.get_json(silent=True) or {}
        pages = int(body.get("pages", pages))
    except Exception:
        pass

    pages = max(1, min(pages, 50))
    result = ingest(pages)
    return _response(200, result)
