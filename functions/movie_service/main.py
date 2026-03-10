"""
CineCloud — Movie Service (GCP Cloud Functions)
================================================
Replaces: AWS Lambda + DynamoDB
Uses:      Firestore (movies collection)

Entry point: http_handler
Deploy:      gcloud functions deploy cinecloud-movies --entry-point=http_handler ...
"""

import json
import os

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
}


def _response(status_code: int, body):
    return (json.dumps(body, default=str), status_code, CORS_HEADERS)


def _qsp(request, key: str, default=None):
    return request.args.get(key, default)


# ── Handlers ──────────────────────────────────────────────────

def list_movies(request) -> tuple:
    """GET /movies[?genre=Action&limit=20&nextToken=...]"""
    limit = min(int(_qsp(request, "limit", 20)), 50)
    genre = _qsp(request, "genre")

    query = _db.collection("movies")

    if genre:
        query = query.where(filter=FieldFilter("genre", "==", genre))

    query = query.limit(limit)
    docs = list(query.stream())
    movies = [d.to_dict() for d in docs]

    return _response(200, {"movies": movies, "count": len(movies)})


def get_movie(request, movie_id: str) -> tuple:
    """GET /movies/{movieId}"""
    doc = _db.collection("movies").document(movie_id).get()
    if not doc.exists:
        return _response(404, {"error": "Movie not found"})
    return _response(200, {"movie": doc.to_dict()})


def search_movies(request) -> tuple:
    """GET /movies/search?q=term — scan + filter (demo-grade search)."""
    q = (_qsp(request, "q") or "").strip().lower()
    if len(q) < 2:
        return _response(400, {"error": "Search query must be at least 2 characters"})

    # Fetch a batch and filter in-memory.
    # For production, replace with Algolia / Typesense / Cloud Search.
    docs = list(_db.collection("movies").limit(300).stream())
    results = []
    for doc in docs:
        m = doc.to_dict()
        if (
            q in m.get("titleLower", "")
            or q in m.get("overviewLower", "")
            or q in m.get("castSearch", "")
        ):
            results.append(m)

    results.sort(key=lambda m: float(m.get("popularity", 0)), reverse=True)
    return _response(200, {"movies": results[:20], "count": len(results[:20])})


def popular_movies(request) -> tuple:
    """GET /movies/popular[?limit=20]"""
    limit = min(int(_qsp(request, "limit", 20)), 50)

    # Fetch a set and sort by popularity in-memory
    docs = list(_db.collection("movies").limit(200).stream())
    movies = [d.to_dict() for d in docs]
    movies.sort(key=lambda m: float(m.get("popularity", 0)), reverse=True)

    return _response(200, {"movies": movies[:limit], "count": limit})


def list_genres(request) -> tuple:
    """GET /movies/genres — Return all distinct genres."""
    docs = list(_db.collection("movies").select(["genres"]).limit(500).stream())
    genre_set: set = set()
    for doc in docs:
        for g in doc.to_dict().get("genres", []):
            genre_set.add(g)
    return _response(200, {"genres": sorted(genre_set)})


# ── Router / Entry Point ──────────────────────────────────────

def http_handler(request):
    """Cloud Function HTTP entry point for all /movies/* routes."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    path = request.path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if path.endswith("/genres"):
        return list_genres(request)
    if path.endswith("/popular"):
        return popular_movies(request)
    if path.endswith("/search"):
        return search_movies(request)

    # /movies/{movieId}
    if segments:
        movie_id = segments[-1]
        if movie_id != "movies":
            return get_movie(request, movie_id)

    # /movies
    return list_movies(request)
