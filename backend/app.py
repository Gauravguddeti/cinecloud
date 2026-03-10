"""
CineCloud — Flask Backend (Clerk + NeonDB edition)
==================================================
Auth:     Clerk (JWT RS256 via JWKS)
Database: NeonDB (PostgreSQL via psycopg2)
Cache:    Upstash Redis (30 min) + recommendations table (24 h fallback)
Deploy:   Render.com free tier
"""

import base64
import json
import math
import os
import threading
import time
from collections import defaultdict
from datetime import datetime

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor
import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    import redis as redis_lib
    _redis_available = True
except ImportError:
    _redis_available = False

# ── Config ────────────────────────────────────────────────────
DATABASE_URL       = os.environ.get("DATABASE_URL", "")
CLERK_JWKS_URL     = os.environ.get("CLERK_JWKS_URL", "")
REDIS_URL          = os.environ.get("REDIS_URL", "")
TMDB_API_KEY       = os.environ.get("TMDB_API_KEY", "")
TOP_N              = 20
CF_WEIGHT          = 0.70
CBF_WEIGHT         = 0.30
MIN_RATINGS_FOR_CF = 3
CACHE_TTL_REDIS    = 1800   # 30 min
CACHE_TTL_DB       = 86400  # 24 h

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


# ── PostgreSQL connection pool ────────────────────────────────
_db_pool = None


def _get_db_pool():
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        _db_pool = pg_pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
    return _db_pool


def _get_conn():
    pool = _get_db_pool()
    if pool is None:
        raise RuntimeError("DATABASE_URL not configured")
    return pool.getconn()


def _put_conn(conn):
    pool = _get_db_pool()
    if pool:
        pool.putconn(conn)


# ── Init DB tables on startup ─────────────────────────────────
def init_db():
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    TEXT PRIMARY KEY,
                    email      TEXT UNIQUE NOT NULL,
                    name       TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS movies (
                    movie_id       TEXT PRIMARY KEY,
                    title          TEXT,
                    title_lower    TEXT,
                    overview       TEXT,
                    overview_lower TEXT,
                    genres         TEXT[],
                    cast_members   TEXT[],
                    cast_search    TEXT,
                    keywords       TEXT[],
                    poster_path    TEXT,
                    backdrop_path  TEXT,
                    release_year   TEXT,
                    vote_average   FLOAT DEFAULT 0,
                    vote_count     INT   DEFAULT 0,
                    popularity     FLOAT DEFAULT 0,
                    runtime        INT,
                    updated_at     TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS ratings (
                    id         SERIAL PRIMARY KEY,
                    user_id    TEXT REFERENCES users(user_id),
                    movie_id   TEXT,
                    rating     FLOAT,
                    rated_at   TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, movie_id)
                );
                CREATE TABLE IF NOT EXISTS recommendations (
                    user_id     TEXT PRIMARY KEY,
                    recs        JSONB DEFAULT '[]',
                    computed_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS events (
                    id         SERIAL PRIMARY KEY,
                    user_id    TEXT,
                    event_type TEXT,
                    properties JSONB DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings(user_id);
                CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity DESC);
                CREATE INDEX IF NOT EXISTS idx_movies_title_lower ON movies(title_lower);
            """)
            # Fuzzy search — safe to run multiple times
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin(title_lower gin_trgm_ops);")
            conn.commit()
        print("[db] Tables ready")
    except Exception as e:
        conn.rollback()
        print(f"[db] Init error: {e}")
    finally:
        _put_conn(conn)


# ── Keep-alive thread (prevents Render free tier sleep) ───────
def _keep_alive_loop():
    self_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not self_url:
        print("[keep-alive] RENDER_EXTERNAL_URL not set — skipping")
        return
    print(f"[keep-alive] Backup self-ping every 14 min ({self_url}/ping)")
    while True:
        time.sleep(14 * 60)
        try:
            requests.get(f"{self_url}/ping", timeout=10)
            print(f"[keep-alive] Pinged at {datetime.utcnow().isoformat()}")
        except Exception as e:
            print(f"[keep-alive] Ping failed: {e}")


threading.Thread(target=_keep_alive_loop, daemon=True).start()


# ── Redis helper ──────────────────────────────────────────────
def _get_redis():
    if not _redis_available or not REDIS_URL:
        return None
    try:
        r = redis_lib.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as e:
        print(f"[redis] connect failed: {e}")
        return None


# ── Clerk JWT verification ────────────────────────────────────
_jwks_cache = {"keys": {}, "fetched_at": 0}
_JWKS_TTL   = 3600  # reload JWKS every hour


def _get_jwks():
    now = time.time()
    if _jwks_cache["keys"] and now - _jwks_cache["fetched_at"] < _JWKS_TTL:
        return _jwks_cache["keys"]
    if not CLERK_JWKS_URL:
        return {}
    try:
        resp = requests.get(CLERK_JWKS_URL, timeout=10)
        if resp.ok:
            keys = {k["kid"]: k for k in resp.json().get("keys", []) if "kid" in k}
            _jwks_cache["keys"] = keys
            _jwks_cache["fetched_at"] = now
            return keys
    except Exception as e:
        print(f"[jwks] Fetch failed: {e}")
    return _jwks_cache["keys"]  # return stale on error


def _b64url_to_int(s):
    pad = 4 - len(s) % 4
    if pad != 4:
        s += "=" * pad
    return int.from_bytes(base64.urlsafe_b64decode(s), "big")


def _jwk_to_public_key(jwk):
    n = _b64url_to_int(jwk["n"])
    e = _b64url_to_int(jwk["e"])
    return RSAPublicNumbers(e, n).public_key(default_backend())


def _verify_token():
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    token = header[7:]
    try:
        unverified = pyjwt.get_unverified_header(token)
        kid        = unverified.get("kid")
        keys       = _get_jwks()
        if not keys:
            # No JWKS available — decode unverified (dev/fallback only)
            payload = pyjwt.decode(token, options={"verify_signature": False})
            return payload.get("sub")
        jwk = keys.get(kid)
        if not jwk:
            # kid not cached — force refresh once
            _jwks_cache["fetched_at"] = 0
            keys = _get_jwks()
            jwk  = keys.get(kid)
        if not jwk:
            return None
        public_key = _jwk_to_public_key(jwk)
        payload    = pyjwt.decode(
            token, public_key,
            algorithms=["RS256"],
            options={"verify_aud": False},
        )
        return payload.get("sub")
    except Exception as e:
        print(f"[auth] verify error: {e}")
        return None


def _unauth():
    return jsonify({"error": "Unauthorized"}), 401


# ── Start up ──────────────────────────────────────────────────
try:
    init_db()
except Exception as _e:
    print(f"[startup] DB init skipped: {_e}")


# =============================================================
#  AUTH
# =============================================================

@app.route("/auth/sync", methods=["POST"])
def auth_sync():
    """Called by frontend after Clerk sign-in to upsert user in NeonDB."""
    uid = _verify_token()
    if not uid:
        return _unauth()
    body  = request.get_json() or {}
    email = body.get("email", "").strip()
    name  = body.get("name", "").strip()
    if not email:
        return jsonify({"error": "email required"}), 400
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO users (user_id, email, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                    SET email = EXCLUDED.email,
                        name  = COALESCE(NULLIF(EXCLUDED.name, ''), users.name)
                RETURNING *
            """, (uid, email, name))
            user = dict(cur.fetchone())
            conn.commit()
        return jsonify({"user": _user_resp(user)})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        _put_conn(conn)


@app.route("/auth/profile", methods=["GET"])
def auth_profile():
    uid = _verify_token()
    if not uid:
        return _unauth()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (uid,))
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "User not found — call /auth/sync first"}), 404
        return jsonify({"user": _user_resp(dict(row))})
    finally:
        _put_conn(conn)


def _user_resp(row):
    created = row.get("created_at")
    return {
        "userId":    row["user_id"],
        "email":     row["email"],
        "name":      row.get("name", ""),
        "createdAt": created.isoformat() if hasattr(created, "isoformat") else str(created or ""),
    }


# =============================================================
#  MOVIES
# =============================================================

@app.route("/movies/list", methods=["GET"])
def movies_list():
    genre      = request.args.get("genre")
    limit      = min(int(request.args.get("limit", 50)), 200)
    next_token = request.args.get("nextToken")
    offset     = int(next_token) if next_token and next_token.isdigit() else 0
    conn       = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if genre:
                cur.execute(
                    "SELECT * FROM movies WHERE %s = ANY(genres) ORDER BY popularity DESC LIMIT %s OFFSET %s",
                    (genre, limit, offset),
                )
            else:
                cur.execute(
                    "SELECT * FROM movies ORDER BY popularity DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            movies = [_movie_resp(dict(r)) for r in cur.fetchall()]
        next_offset = offset + len(movies)
        has_more    = len(movies) == limit
        return jsonify({
            "movies":    movies,
            "count":     len(movies),
            "nextToken": str(next_offset) if has_more else None,
        })
    finally:
        _put_conn(conn)


@app.route("/movies/search", methods=["GET"])
def movies_search():
    q = request.args.get("q", "").lower().strip()
    if not q:
        return jsonify({"movies": [], "count": 0})
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Fuzzy trigram + LIKE hybrid — tolerates typos (jumannjai → Jumanji)
            cur.execute("""
                SELECT *
                FROM movies
                WHERE word_similarity(%s, title_lower) > 0.2
                   OR title_lower LIKE %s
                   OR cast_search LIKE %s
                ORDER BY
                    CASE WHEN title_lower = %s    THEN 4
                         WHEN title_lower LIKE %s THEN 3
                         WHEN title_lower LIKE %s THEN 2
                         ELSE 1 END DESC,
                    word_similarity(%s, title_lower) DESC,
                    popularity DESC
                LIMIT 20
            """, (
                q, f"%{q}%", f"%{q}%",   # WHERE
                q, f"{q}%", f"%{q}%",    # ORDER CASE
                q,                        # final similarity sort
            ))
            movies = [_movie_resp(dict(r)) for r in cur.fetchall()]

        # If sparse results, fire background TMDB import — returns instantly, next search will find them
        if len(movies) < 4 and TMDB_API_KEY:
            threading.Thread(target=_background_tmdb_import, args=(q,), daemon=True).start()

        return jsonify({"movies": movies, "count": len(movies)})
    finally:
        _put_conn(conn)


def _background_tmdb_import(query: str):
    """Fetch from TMDB for a search query and upsert into DB without blocking the response."""
    genre_map = _fetch_genre_map()
    try:
        for lang in ("en-US", "hi-IN"):
            resp = requests.get(
                "https://api.themoviedb.org/3/search/movie",
                params={"api_key": TMDB_API_KEY, "query": query, "language": lang, "page": 1},
                timeout=10,
            )
            if not resp.ok:
                continue
            batch = []
            for item in resp.json().get("results", [])[:10]:
                mid   = str(item["id"])
                title = item.get("title") or item.get("original_title", "")
                genres = [genre_map.get(gid, "") for gid in item.get("genre_ids", []) if gid in genre_map]
                batch.append({
                    "movie_id":       mid,
                    "title":          title,
                    "title_lower":    title.lower(),
                    "overview":       item.get("overview", ""),
                    "overview_lower": item.get("overview", "").lower(),
                    "genres":         genres,
                    "cast_members":   [],
                    "cast_search":    "",
                    "keywords":       [],
                    "poster_path":    item.get("poster_path"),
                    "backdrop_path":  item.get("backdrop_path"),
                    "release_year":   (item.get("release_date") or "")[:4],
                    "vote_average":   float(item.get("vote_average", 0)),
                    "vote_count":     int(item.get("vote_count", 0)),
                    "popularity":     float(item.get("popularity", 0)),
                    "runtime":        None,
                    "updated_at":     datetime.utcnow(),
                })
            if batch:
                _write_pg_batch(batch)
    except Exception as e:
        print(f"[bg_import] {e}")


@app.route("/movies/popular", methods=["GET"])
def movies_popular():
    limit = min(int(request.args.get("limit", 20)), 100)
    conn  = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM movies ORDER BY popularity DESC LIMIT %s", (limit,))
            movies = [_movie_resp(dict(r)) for r in cur.fetchall()]
        return jsonify({"movies": movies, "count": len(movies)})
    finally:
        _put_conn(conn)


@app.route("/movies/genres", methods=["GET"])
def movies_genres():
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT unnest(genres) AS g FROM movies ORDER BY g")
            genres = [r[0] for r in cur.fetchall()]
        return jsonify({"genres": genres})
    finally:
        _put_conn(conn)


@app.route("/movies/detail/<movie_id>", methods=["GET"])
def movies_detail(movie_id):
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "Movie not found"}), 404
        return jsonify({"movie": _movie_resp(dict(row))})
    finally:
        _put_conn(conn)


def _movie_resp(row):
    return {
        "movieId":      row["movie_id"],
        "title":        row.get("title", ""),
        "titleLower":   row.get("title_lower", ""),
        "overview":     row.get("overview", ""),
        "genres":       row.get("genres") or [],
        "cast":         row.get("cast_members") or [],
        "castSearch":   row.get("cast_search", ""),
        "keywords":     row.get("keywords") or [],
        "posterPath":   ("https://image.tmdb.org/t/p/w500" + row["poster_path"]) if row.get("poster_path") else None,
        "backdropPath": ("https://image.tmdb.org/t/p/w1280" + row["backdrop_path"]) if row.get("backdrop_path") else None,
        "releaseYear":  row.get("release_year", ""),
        "voteAverage":  float(row.get("vote_average") or 0),
        "voteCount":    int(row.get("vote_count") or 0),
        "popularity":   float(row.get("popularity") or 0),
        "runtime":      row.get("runtime"),
    }


# =============================================================
#  RATINGS
# =============================================================

@app.route("/ratings/submit", methods=["POST"])
def ratings_submit():
    uid = _verify_token()
    if not uid:
        return _unauth()
    body     = request.get_json() or {}
    movie_id = body.get("movieId")
    rating   = body.get("rating")
    if not movie_id or rating is None:
        return jsonify({"error": "movieId and rating required"}), 400
    rating = float(rating)
    if not 1 <= rating <= 5:
        return jsonify({"error": "Rating must be 1-5"}), 400
    now  = datetime.utcnow()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ratings (user_id, movie_id, rating, rated_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, movie_id) DO UPDATE
                    SET rating = EXCLUDED.rating, updated_at = EXCLUDED.updated_at
            """, (uid, movie_id, rating, now, now))
            conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        _put_conn(conn)
    threading.Thread(target=_recompute_recs, args=(uid,), daemon=True).start()
    data = {"userId": uid, "movieId": movie_id, "rating": rating, "ratedAt": now.isoformat()}
    return jsonify({"message": "Rating submitted", "rating": data})


@app.route("/ratings/user/<user_id>", methods=["GET"])
def ratings_get_user(user_id):
    uid = _verify_token()
    if not uid or uid != user_id:
        return _unauth()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ratings WHERE user_id = %s ORDER BY updated_at DESC", (user_id,)
            )
            ratings = [{
                "userId":  r["user_id"],
                "movieId": r["movie_id"],
                "rating":  float(r["rating"]),
                "ratedAt": r["rated_at"].isoformat() if hasattr(r.get("rated_at"), "isoformat") else str(r.get("rated_at", "")),
            } for r in cur.fetchall()]
        return jsonify({"ratings": ratings, "count": len(ratings)})
    finally:
        _put_conn(conn)


# =============================================================
#  RECOMMENDATIONS
# =============================================================

@app.route("/recommendations/<user_id>", methods=["GET"])
def recommendations_get(user_id):
    uid = _verify_token()
    if not uid or uid != user_id:
        return _unauth()
    cached = _cache_get(user_id)
    if cached:
        return jsonify({"recommendations": cached, "userId": user_id,
                        "fromCache": True, "count": len(cached)})
    t0              = time.time()
    all_ratings     = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs            = compute_recommendations(user_id, all_ratings, movies_metadata)
    elapsed         = round(time.time() - t0, 3)
    _cache_set(user_id, recs)
    return jsonify({"recommendations": recs, "userId": user_id,
                    "fromCache": False, "computeTimeSeconds": elapsed, "count": len(recs)})


@app.route("/recommendations/<user_id>/refresh", methods=["POST"])
def recommendations_refresh(user_id):
    uid = _verify_token()
    if not uid or uid != user_id:
        return _unauth()
    _cache_invalidate(user_id)
    all_ratings     = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs            = compute_recommendations(user_id, all_ratings, movies_metadata)
    _cache_set(user_id, recs)
    return jsonify({"recommendations": recs, "userId": user_id,
                    "fromCache": False, "count": len(recs)})


# =============================================================
#  ETL — TMDB INGEST
# =============================================================

_ingest_status = {"running": False, "total": 0, "message": "idle"}

def _fetch_genre_map():
    """Fetch TMDB genre ID→name mapping once for EN and HI."""
    genre_map = {}
    for lang in ("en-US", "hi-IN"):
        r = requests.get(
            "https://api.themoviedb.org/3/genre/movie/list",
            params={"api_key": TMDB_API_KEY, "language": lang},
            timeout=10,
        )
        if r.ok:
            for g in r.json().get("genres", []):
                genre_map[g["id"]] = g["name"]
    return genre_map

@app.route("/admin/ingest", methods=["POST"])
def admin_ingest():
    global _ingest_status
    if _ingest_status["running"]:
        return jsonify({"message": "Ingest already running", "status": _ingest_status})
    body  = request.get_json() or {}
    pages = min(int(body.get("pages", 5)), 50)
    if not TMDB_API_KEY:
        return jsonify({"error": "TMDB_API_KEY not set"}), 500
    # Run in background so Render's 30s timeout doesn't kill it
    threading.Thread(target=_run_ingest, args=(pages,), daemon=True).start()
    return jsonify({"message": f"Ingest started ({pages} pages × 4 sources)", "status": "started"})

@app.route("/admin/ingest/status", methods=["GET"])
def admin_ingest_status():
    return jsonify(_ingest_status)

def _run_ingest(pages: int):
    global _ingest_status
    _ingest_status = {"running": True, "total": 0, "message": "fetching genre map..."}
    # Use a dedicated fresh connection — avoids stale pool connections in background threads
    conn = psycopg2.connect(DATABASE_URL)
    try:
        genre_map = _fetch_genre_map()
        SOURCES = [
            {"endpoint": "movie/popular",   "language": "en-US", "region": None},
            {"endpoint": "movie/top_rated", "language": "en-US", "region": None},
            {"endpoint": "movie/popular",   "language": "hi-IN", "region": "IN"},
            {"endpoint": "movie/top_rated", "language": "hi-IN", "region": "IN"},
        ]
        batch = []
        seen  = set()
        for source in SOURCES:
            for page in range(1, pages + 1):
                params = {"api_key": TMDB_API_KEY, "page": page, "language": source["language"]}
                if source["region"]:
                    params["region"] = source["region"]
                resp = requests.get(
                    f"https://api.themoviedb.org/3/{source['endpoint']}",
                    params=params, timeout=15,
                )
                if not resp.ok:
                    continue
                for movie in resp.json().get("results", []):
                    mid = str(movie["id"])
                    if mid in seen:
                        continue
                    seen.add(mid)
                    genres = [genre_map.get(gid, "") for gid in movie.get("genre_ids", []) if gid in genre_map]
                    title  = movie.get("title") or movie.get("original_title", "")
                    batch.append({
                        "movie_id":       mid,
                        "title":          title,
                        "title_lower":    title.lower(),
                        "overview":       movie.get("overview", ""),
                        "overview_lower": movie.get("overview", "").lower(),
                        "genres":         genres,
                        "cast_members":   [],
                        "cast_search":    "",
                        "keywords":       [],
                        "poster_path":    movie.get("poster_path"),
                        "backdrop_path":  movie.get("backdrop_path"),
                        "release_year":   (movie.get("release_date") or "")[:4],
                        "vote_average":   float(movie.get("vote_average", 0)),
                        "vote_count":     int(movie.get("vote_count", 0)),
                        "popularity":     float(movie.get("popularity", 0)),
                        "runtime":        None,
                        "updated_at":     datetime.utcnow(),
                    })
                if len(batch) >= 500:
                    _flush_batch(conn, batch)
                    batch = []
            _ingest_status["message"] = f"processed {source['endpoint']} ({source['language']})"
        if batch:
            _flush_batch(conn, batch)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM movies")
            total = cur.fetchone()[0]
        _ingest_status = {"running": False, "total": total, "message": f"Done — {total} movies in DB"}
    except Exception as e:
        _ingest_status = {"running": False, "total": 0, "message": f"Error: {e}"}
        print(f"[ingest] {e}")
    finally:
        conn.close()


def _flush_batch(conn, movies):
    """Write a batch using an already-open connection (used by _run_ingest)."""
    try:
        with conn.cursor() as cur:
            for m in movies:
                cur.execute("""
                    INSERT INTO movies (
                        movie_id, title, title_lower, overview, overview_lower,
                        genres, cast_members, cast_search, keywords,
                        poster_path, backdrop_path, release_year,
                        vote_average, vote_count, popularity, runtime, updated_at
                    ) VALUES (
                        %(movie_id)s, %(title)s, %(title_lower)s, %(overview)s, %(overview_lower)s,
                        %(genres)s, %(cast_members)s, %(cast_search)s, %(keywords)s,
                        %(poster_path)s, %(backdrop_path)s, %(release_year)s,
                        %(vote_average)s, %(vote_count)s, %(popularity)s, %(runtime)s, %(updated_at)s
                    )
                    ON CONFLICT (movie_id) DO UPDATE SET
                        title          = EXCLUDED.title,
                        title_lower    = EXCLUDED.title_lower,
                        overview       = EXCLUDED.overview,
                        overview_lower = EXCLUDED.overview_lower,
                        genres         = EXCLUDED.genres,
                        poster_path    = EXCLUDED.poster_path,
                        backdrop_path  = EXCLUDED.backdrop_path,
                        release_year   = EXCLUDED.release_year,
                        vote_average   = EXCLUDED.vote_average,
                        vote_count     = EXCLUDED.vote_count,
                        popularity     = EXCLUDED.popularity,
                        updated_at     = EXCLUDED.updated_at
                """, m)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ingest] flush error: {e}")


def _write_pg_batch(movies):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for m in movies:
                cur.execute("""
                    INSERT INTO movies (
                        movie_id, title, title_lower, overview, overview_lower,
                        genres, cast_members, cast_search, keywords,
                        poster_path, backdrop_path, release_year,
                        vote_average, vote_count, popularity, runtime, updated_at
                    ) VALUES (
                        %(movie_id)s, %(title)s, %(title_lower)s, %(overview)s, %(overview_lower)s,
                        %(genres)s, %(cast_members)s, %(cast_search)s, %(keywords)s,
                        %(poster_path)s, %(backdrop_path)s, %(release_year)s,
                        %(vote_average)s, %(vote_count)s, %(popularity)s, %(runtime)s, %(updated_at)s
                    )
                    ON CONFLICT (movie_id) DO UPDATE SET
                        title          = EXCLUDED.title,
                        title_lower    = EXCLUDED.title_lower,
                        overview       = EXCLUDED.overview,
                        overview_lower = EXCLUDED.overview_lower,
                        genres         = EXCLUDED.genres,
                        cast_members   = EXCLUDED.cast_members,
                        cast_search    = EXCLUDED.cast_search,
                        keywords       = EXCLUDED.keywords,
                        poster_path    = EXCLUDED.poster_path,
                        backdrop_path  = EXCLUDED.backdrop_path,
                        release_year   = EXCLUDED.release_year,
                        vote_average   = EXCLUDED.vote_average,
                        vote_count     = EXCLUDED.vote_count,
                        popularity     = EXCLUDED.popularity,
                        runtime        = EXCLUDED.runtime,
                        updated_at     = EXCLUDED.updated_at
                """, m)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ingest] batch write failed: {e}")
    finally:
        _put_conn(conn)


# =============================================================
#  EVENTS & HEALTH
# =============================================================

@app.route("/events/track", methods=["POST"])
def events_track():
    uid = _verify_token()
    if not uid:
        return _unauth()
    body = request.get_json() or {}
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO events (user_id, event_type, properties) VALUES (%s, %s, %s)",
                (uid, body.get("eventType", ""), json.dumps(body.get("properties", {}))),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[events] track error: {e}")
    finally:
        _put_conn(conn)
    return jsonify({"tracked": True})


@app.route("/ping", methods=["GET", "POST"])
def ping():
    return jsonify({"status": "ok", "ts": int(time.time())})


@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "CineCloud API", "status": "running", "version": "3.0-clerk-neon"})


# =============================================================
#  CACHE HELPERS  (Redis primary, recommendations table fallback)
# =============================================================

def _cache_get(user_id):
    r = _get_redis()
    if r:
        try:
            cached = r.get(f"rec:{user_id}")
            if cached:
                return json.loads(cached)
        except Exception:
            pass
    # DB fallback — use if less than 24 h old
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT recs, computed_at FROM recommendations WHERE user_id = %s", (user_id,)
            )
            row = cur.fetchone()
        if row:
            age = (datetime.utcnow() - row["computed_at"]).total_seconds()
            if age < CACHE_TTL_DB:
                return row["recs"]
    except Exception:
        pass
    finally:
        _put_conn(conn)
    return None


def _cache_set(user_id, recs):
    r = _get_redis()
    if r:
        try:
            r.setex(f"rec:{user_id}", CACHE_TTL_REDIS, json.dumps(recs))
        except Exception:
            pass
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO recommendations (user_id, recs, computed_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE
                    SET recs = EXCLUDED.recs, computed_at = EXCLUDED.computed_at
            """, (user_id, json.dumps(recs)))
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[cache] DB write failed: {e}")
    finally:
        _put_conn(conn)


def _cache_invalidate(user_id):
    r = _get_redis()
    if r:
        try:
            r.delete(f"rec:{user_id}")
        except Exception:
            pass
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM recommendations WHERE user_id = %s", (user_id,))
            conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        _put_conn(conn)


# =============================================================
#  DATA LOADERS (PostgreSQL)
# =============================================================

def _load_all_ratings():
    ratings = defaultdict(dict)
    conn    = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, movie_id, rating FROM ratings")
            for user_id, movie_id, rating in cur.fetchall():
                ratings[user_id][movie_id] = float(rating)
    finally:
        _put_conn(conn)
    return dict(ratings)


def _load_movies_metadata():
    movies = {}
    conn   = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM movies")
            for row in cur.fetchall():
                m = _movie_resp(dict(row))
                movies[m["movieId"]] = m
    finally:
        _put_conn(conn)
    return movies


# =============================================================
#  RECOMMENDATION ALGORITHM  — Hybrid CF (70%) + CBF (30%)
# =============================================================

def _recompute_recs(user_id):
    """Background thread worker."""
    try:
        _cache_invalidate(user_id)
        recs = compute_recommendations(user_id, _load_all_ratings(), _load_movies_metadata())
        _cache_set(user_id, recs)
        print(f"[worker] {len(recs)} recs for {user_id} written to DB")
    except Exception as e:
        print(f"[worker] Error for {user_id}: {e}")


def _cosine_similarity(vec_a, vec_b):
    common = set(vec_a) & set(vec_b)
    if not common:
        return 0.0
    dot    = sum(vec_a[m] * vec_b[m] for m in common)
    norm_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _collaborative_filter(user_id, all_ratings, top_k=15):
    user_ratings = all_ratings.get(user_id, {})
    if len(user_ratings) < MIN_RATINGS_FOR_CF:
        return {}
    sims = []
    for other_id, other_r in all_ratings.items():
        if other_id == user_id:
            continue
        s = _cosine_similarity(user_ratings, other_r)
        if s > 0:
            sims.append((s, other_id))
    sims.sort(reverse=True)
    scores, sim_sums = defaultdict(float), defaultdict(float)
    for sim, other_id in sims[:top_k]:
        for mid, rating in all_ratings[other_id].items():
            if mid not in user_ratings:
                scores[mid]   += sim * rating
                sim_sums[mid] += abs(sim)
    return {mid: scores[mid] / sim_sums[mid] for mid in scores if sim_sums[mid] > 0}


def _movie_feature_vector(movie):
    tf = defaultdict(float)
    for g  in movie.get("genres", []):
        tf[f"genre:{g.lower().replace(' ', '_')}"] += 3.0
    for a in (movie.get("cast") or [])[:5]:
        tf[f"cast:{a.lower().replace(' ', '_')}"]  += 2.0
    for kw in (movie.get("keywords") or [])[:15]:
        tf[f"kw:{kw.lower().replace(' ', '_')}"]   += 1.0
    return dict(tf)


def _user_preference_vector(user_ratings, movies_metadata):
    pref = defaultdict(float)
    for mid, rating in user_ratings.items():
        movie = movies_metadata.get(mid)
        if not movie:
            continue
        w = (rating - 3.0) / 2.0
        for term, tf in _movie_feature_vector(movie).items():
            pref[term] += w * tf
    return dict(pref)


def _content_based_filter(user_id, all_ratings, movies_metadata):
    user_ratings = all_ratings.get(user_id, {})
    if not user_ratings:
        return {}
    pref = _user_preference_vector(user_ratings, movies_metadata)
    if not pref:
        return {}
    scores = {}
    for mid, movie in movies_metadata.items():
        if mid in user_ratings:
            continue
        s = _cosine_similarity(pref, _movie_feature_vector(movie))
        if s > 0:
            scores[mid] = s
    return scores


def _normalise(scores):
    if not scores:
        return {}
    lo, hi = min(scores.values()), max(scores.values())
    spread = hi - lo
    if spread == 0:
        return {k: 1.0 for k in scores}
    return {k: (v - lo) / spread for k, v in scores.items()}


def compute_recommendations(user_id, all_ratings, movies_metadata, top_n=TOP_N):
    user_ratings = all_ratings.get(user_id, {})
    if not user_ratings:
        popular = sorted(movies_metadata.values(),
                         key=lambda m: float(m.get("popularity", 0)), reverse=True)
        return _fmt_recs(popular[:top_n], "Popular on CineCloud")

    cf_norm  = _normalise(_collaborative_filter(user_id, all_ratings))
    cbf_norm = _normalise(_content_based_filter(user_id, all_ratings, movies_metadata))
    candidates = set(cf_norm) | set(cbf_norm)

    if not candidates:
        popular = sorted(movies_metadata.values(),
                         key=lambda m: float(m.get("popularity", 0)), reverse=True)
        return _fmt_recs([m for m in popular if m["movieId"] not in user_ratings][:top_n], "Trending")

    hybrid = sorted([
        (
            cf_norm.get(mid, 0) * CF_WEIGHT + cbf_norm.get(mid, 0) * CBF_WEIGHT,
            mid,
            "Because users like you enjoyed similar movies"
            if cf_norm.get(mid, 0) > cbf_norm.get(mid, 0)
            else "Because you enjoyed similar genres & cast",
        )
        for mid in candidates
    ], reverse=True)

    results = []
    for score, mid, reason in hybrid[:top_n]:
        movie = movies_metadata.get(mid)
        if movie:
            results.append({
                "movieId":     mid,
                "title":       movie.get("title", ""),
                "posterPath":  movie.get("posterPath"),
                "genres":      movie.get("genres", []),
                "voteAverage": float(movie.get("voteAverage", 0)),
                "releaseYear": movie.get("releaseYear", ""),
                "score":       round(score, 4),
                "reason":      reason,
            })
    return results


def _fmt_recs(movies, reason):
    return [{
        "movieId":     m.get("movieId", ""),
        "title":       m.get("title", ""),
        "posterPath":  m.get("posterPath"),
        "genres":      m.get("genres", []),
        "voteAverage": float(m.get("voteAverage", 0)),
        "releaseYear": m.get("releaseYear", ""),
        "score":       float(m.get("popularity", 0)),
        "reason":      reason,
    } for m in movies]


# =============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

