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
import random
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
CF_WEIGHT          = 0.60
CBF_WEIGHT         = 0.40
MIN_RATINGS_FOR_CF = 3
CACHE_TTL_REDIS    = 1800   # 30 min
CACHE_TTL_DB       = 86400  # 24 h
# Quality gates for recommendations
MIN_VOTE_COUNT     = 50     # ignore movies with fewer votes than this
MIN_VOTE_AVG       = 6.0    # ignore movies rated below this average
MIN_CBF_SCORE      = 0.05   # minimum cosine similarity to enter candidate pool
MAX_PER_FRANCHISE  = 3      # diversity cap: max results from same franchise/series

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
                CREATE TABLE IF NOT EXISTS implicit_signals (
                    id         SERIAL PRIMARY KEY,
                    user_id    TEXT NOT NULL,
                    movie_id   TEXT NOT NULL,
                    signal     TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings(user_id);
                CREATE INDEX IF NOT EXISTS idx_implicit_user ON implicit_signals(user_id);
                CREATE INDEX IF NOT EXISTS idx_implicit_movie ON implicit_signals(movie_id);
                CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity DESC);
                CREATE INDEX IF NOT EXISTS idx_movies_title_lower ON movies(title_lower);
            """)
            # Fuzzy search — safe to run multiple times
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin(title_lower gin_trgm_ops);")
            # Clean up null genre elements left by old code (safe to run repeatedly)
            cur.execute("""
                UPDATE movies
                SET genres = ARRAY(SELECT elem FROM UNNEST(genres) AS elem WHERE elem IS NOT NULL AND elem != '')
                WHERE genres IS NOT NULL
                  AND EXISTS (SELECT 1 FROM UNNEST(genres) AS elem WHERE elem IS NULL OR elem = '')
            """)
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

    # In-memory cache — same query within 60 s returns instantly (prevents TMDB hammering on debounce)
    now    = time.time()
    cached = _search_cache.get(q)
    if cached and (now - cached["ts"]) < 60:
        return jsonify({"movies": cached["movies"], "count": len(cached["movies"])})

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
                q, f"%{q}%", f"%{q}%",
                q, f"{q}%", f"%{q}%",
                q,
            ))
            db_movies = [_movie_resp(dict(r)) for r in cur.fetchall()]
    finally:
        _put_conn(conn)

    # Go to TMDB when there's no direct substring match in DB titles
    # (fuzzy can return 4+ irrelevant results, e.g. "lagaan" matching unrelated films)
    has_direct_match = any(q in m.get("titleLower", "") for m in db_movies)
    if not has_direct_match and TMDB_API_KEY:
        tmdb_resp, raw_batch = _tmdb_live_search(q)
        db_ids = {m["movieId"] for m in db_movies}
        extra  = [m for m in tmdb_resp if m["movieId"] not in db_ids]
        movies = db_movies + extra
        if raw_batch:
            threading.Thread(target=_background_tmdb_import, args=(raw_batch,), daemon=True).start()
    else:
        movies = db_movies

    result = movies[:20]
    _search_cache[q] = {"movies": result, "ts": now}
    # Prune entries older than 5 min to prevent unbounded growth
    stale = [k for k, v in list(_search_cache.items()) if (now - v["ts"]) > 300]
    for k in stale:
        _search_cache.pop(k, None)

    return jsonify({"movies": result, "count": len(result)})


def _tmdb_live_search(query: str):
    """Search TMDB movies + TV for query (en + hi) and return (response-shaped list, raw upsert batch)."""
    genre_map = _fetch_genre_map()
    resp_movies, raw_batch, seen = [], [], set()
    for lang in ("en-US", "hi-IN"):
        try:
            r = requests.get(
                "https://api.themoviedb.org/3/search/movie",
                params={"api_key": TMDB_API_KEY, "query": query, "language": lang, "page": 1},
                timeout=8,
            )
        except Exception:
            continue
        if not r.ok:
            continue
        for item in r.json().get("results", [])[:10]:
            mid = str(item["id"])
            if mid in seen:
                continue
            seen.add(mid)
            title  = item.get("title") or item.get("original_title", "")
            genres = [genre_map.get(gid, "") for gid in item.get("genre_ids", []) if gid in genre_map]
            poster_path   = item.get("poster_path")
            backdrop_path = item.get("backdrop_path")
            release_year  = (item.get("release_date") or "")[:4]
            raw_batch.append({
                "movie_id":       mid,
                "title":          title,
                "title_lower":    title.lower(),
                "overview":       item.get("overview", ""),
                "overview_lower": item.get("overview", "").lower(),
                "genres":         genres,
                "cast_members":   [],
                "cast_search":    "",
                "keywords":       [],
                "poster_path":    poster_path,
                "backdrop_path":  backdrop_path,
                "release_year":   release_year,
                "vote_average":   float(item.get("vote_average", 0)),
                "vote_count":     int(item.get("vote_count", 0)),
                "popularity":     float(item.get("popularity", 0)),
                "runtime":        None,
                "updated_at":     datetime.utcnow(),
            })
            resp_movies.append({
                "movieId":      mid,
                "title":        title,
                "titleLower":   title.lower(),
                "overview":     item.get("overview", ""),
                "genres":       genres,
                "cast":         [],
                "castSearch":   "",
                "keywords":     [],
                "posterPath":   ("https://image.tmdb.org/t/p/w500" + poster_path) if poster_path else None,
                "backdropPath": ("https://image.tmdb.org/t/p/w1280" + backdrop_path) if backdrop_path else None,
                "releaseYear":  release_year,
                "voteAverage":  float(item.get("vote_average", 0)),
                "voteCount":    int(item.get("vote_count", 0)),
                "popularity":   float(item.get("popularity", 0)),
                "runtime":      None,
            })
    # Also search TV series (handles "13 Reasons Why", "Stranger Things", "Breaking Bad", etc.)
    _append_tv_results(query, genre_map, seen, resp_movies, raw_batch)
    return resp_movies, raw_batch


def _append_tv_results(query: str, genre_map: dict, seen: set, resp_movies: list, raw_batch: list):
    """Append TMDB TV series results into the shared lists (en + hi)."""
    for lang in ("en-US", "hi-IN"):
        try:
            r = requests.get(
                "https://api.themoviedb.org/3/search/tv",
                params={"api_key": TMDB_API_KEY, "query": query, "language": lang, "page": 1},
                timeout=8,
            )
        except Exception:
            continue
        if not r.ok:
            continue
        for item in r.json().get("results", [])[:5]:
            mid = f"tv_{item['id']}"
            if mid in seen:
                continue
            seen.add(mid)
            title         = item.get("name") or item.get("original_name", "")
            genres        = [genre_map.get(gid, "") for gid in item.get("genre_ids", []) if gid in genre_map]
            poster_path   = item.get("poster_path")
            backdrop_path = item.get("backdrop_path")
            release_year  = (item.get("first_air_date") or "")[:4]
            raw_batch.append({
                "movie_id":       mid,
                "title":          title,
                "title_lower":    title.lower(),
                "overview":       item.get("overview", ""),
                "overview_lower": item.get("overview", "").lower(),
                "genres":         genres,
                "cast_members":   [],
                "cast_search":    "",
                "keywords":       [],
                "poster_path":    poster_path,
                "backdrop_path":  backdrop_path,
                "release_year":   release_year,
                "vote_average":   float(item.get("vote_average", 0)),
                "vote_count":     int(item.get("vote_count", 0)),
                "popularity":     float(item.get("popularity", 0)),
                "runtime":        None,
                "updated_at":     datetime.utcnow(),
            })
            resp_movies.append({
                "movieId":      mid,
                "title":        title,
                "titleLower":   title.lower(),
                "overview":     item.get("overview", ""),
                "genres":       genres,
                "cast":         [],
                "castSearch":   "",
                "keywords":     [],
                "posterPath":   ("https://image.tmdb.org/t/p/w500" + poster_path) if poster_path else None,
                "backdropPath": ("https://image.tmdb.org/t/p/w1280" + backdrop_path) if backdrop_path else None,
                "releaseYear":  release_year,
                "voteAverage":  float(item.get("vote_average", 0)),
                "voteCount":    int(item.get("vote_count", 0)),
                "popularity":   float(item.get("popularity", 0)),
                "runtime":      None,
            })


def _background_tmdb_import(raw_batch: list):
    """Upsert a pre-built batch into DB without blocking the response."""
    if not raw_batch:
        return
    try:
        dsn = DATABASE_URL
        if "sslmode" not in dsn:
            dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
        conn = psycopg2.connect(dsn)
        try:
            _flush_batch(conn, raw_batch)
        finally:
            conn.close()
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
        "genres":       [g for g in (row.get("genres") or []) if g],
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
    threading.Thread(target=_enrich_and_recompute, args=(uid, movie_id), daemon=True).start()
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


@app.route("/ratings/delete/<movie_id>", methods=["DELETE"])
def rating_delete(movie_id):
    uid = _verify_token()
    if not uid:
        return _unauth()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ratings WHERE user_id = %s AND movie_id = %s", (uid, movie_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        _put_conn(conn)
    threading.Thread(target=_recompute_recs, args=(uid,), daemon=True).start()
    return jsonify({"message": "Rating removed"})


@app.route("/ratings/reset", methods=["DELETE"])
def ratings_reset():
    uid = _verify_token()
    if not uid:
        return _unauth()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ratings WHERE user_id = %s", (uid,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        _put_conn(conn)
    threading.Thread(target=_recompute_recs, args=(uid,), daemon=True).start()
    return jsonify({"message": "All ratings cleared"})


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
    implicit        = _load_implicit_signals(user_id)
    recs            = compute_recommendations(user_id, all_ratings, movies_metadata, implicit=implicit)
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
    implicit        = _load_implicit_signals(user_id)
    recs            = compute_recommendations(user_id, all_ratings, movies_metadata, implicit=implicit)
    _cache_set(user_id, recs)
    return jsonify({"recommendations": recs, "userId": user_id,
                    "fromCache": False, "count": len(recs)})


# =============================================================
#  ETL — TMDB INGEST
# =============================================================

_search_cache: dict = {}  # query → {"movies": list, "ts": float}; 60 s TTL, auto-pruned
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
    conn = None
    try:
        # Fresh direct connection — avoids stale pool connections in background threads
        dsn = DATABASE_URL
        if "sslmode" not in dsn:
            dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
        conn = psycopg2.connect(dsn)
        _ingest_status["message"] = "connected to DB, fetching genre map..."
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
        print(f"[ingest] FATAL {type(e).__name__}: {e}")
    finally:
        if conn:
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
            event_type = body.get("eventType", "")
            movie_id   = str(body.get("properties", {}).get("movieId", ""))
            if event_type in {"movie_view", "search_click", "browse_hover"} and movie_id:
                cur.execute(
                    "INSERT INTO implicit_signals (user_id, movie_id, signal) VALUES (%s, %s, %s)",
                    (uid, movie_id, event_type),
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
    """Load all ratings with temporal decay applied.
    Recent ratings count more — decay 3% per month beyond 30 days.
    Ratings older than 2 years get capped at 40% of original weight.
    """
    ratings = defaultdict(dict)
    conn    = _get_conn()
    now     = datetime.utcnow()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, movie_id, rating, updated_at FROM ratings")
            for user_id, movie_id, rating, updated_at in cur.fetchall():
                age_days = (now - updated_at).days if updated_at else 0
                # Decay: full weight within 30 days, then -3%/month, floor at 0.4
                months_old = max(0, (age_days - 30) / 30.0)
                decay      = max(0.4, 1.0 - 0.03 * months_old)
                ratings[user_id][movie_id] = float(rating) * decay
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


def _load_implicit_signals(user_id: str) -> dict:
    """Load implicit event weights for a user: movie_id → score.
    Signals and weights:
      movie_view       → 1.0  (opened detail modal)
      search_click     → 0.7  (clicked search result)
      browse_hover     → 0.3  (hovered on poster ≥ 2s)
    Recency decay: same 3%/month formula as ratings.
    Returns normalised dict (max = 1.0).
    """
    SIGNAL_WEIGHTS = {"movie_view": 1.0, "search_click": 0.7, "browse_hover": 0.3}
    raw: dict = defaultdict(float)
    conn      = _get_conn()
    now       = datetime.utcnow()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT movie_id, signal, created_at FROM implicit_signals WHERE user_id = %s",
                (user_id,),
            )
            for movie_id, signal, created_at in cur.fetchall():
                w          = SIGNAL_WEIGHTS.get(signal, 0.3)
                age_days   = (now - created_at).days if created_at else 0
                months_old = max(0, (age_days - 7) / 30.0)   # grace period: 7 days
                decay      = max(0.2, 1.0 - 0.05 * months_old)
                raw[movie_id] += w * decay
    finally:
        _put_conn(conn)
    if not raw:
        return {}
    max_val = max(raw.values())
    return {mid: v / max_val for mid, v in raw.items()}


# =============================================================
#  RECOMMENDATION ALGORITHM  — Hybrid CF (70%) + CBF (30%)
# =============================================================

def _ensure_movie_in_db(movie_id: str):
    """Fetch full TMDB data (genres, cast, keywords) for a movie and upsert into movies table.
    Movies found via live TMDB search are stored with empty cast/keywords — this fixes that."""
    if movie_id.startswith("tv_"):
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM movies
                WHERE movie_id = %s
                  AND genres IS NOT NULL
                  AND cardinality(genres) > 0
                  AND genres[1] IS NOT NULL
            """, (movie_id,))
            if cur.fetchone():
                return  # Already has proper genre data
    finally:
        _put_conn(conn)
    try:
        mr = requests.get(
            f"https://api.themoviedb.org/3/movie/{movie_id}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"},
            timeout=10,
        )
        if not mr.ok:
            return
        m = mr.json()
        genres = [g["name"] for g in m.get("genres", [])]
        cr = requests.get(
            f"https://api.themoviedb.org/3/movie/{movie_id}/credits",
            params={"api_key": TMDB_API_KEY},
            timeout=10,
        )
        cast_members = [c["name"] for c in cr.json().get("cast", [])[:10]] if cr.ok else []
        kr = requests.get(
            f"https://api.themoviedb.org/3/movie/{movie_id}/keywords",
            params={"api_key": TMDB_API_KEY},
            timeout=10,
        )
        keywords = [k["name"] for k in kr.json().get("keywords", [])[:20]] if kr.ok else []
        title = m.get("title") or m.get("original_title", "")
        _write_pg_batch([{
            "movie_id":       movie_id,
            "title":          title,
            "title_lower":    title.lower(),
            "overview":       m.get("overview", ""),
            "overview_lower": m.get("overview", "").lower(),
            "genres":         genres,
            "cast_members":   cast_members,
            "cast_search":    " ".join(cast_members).lower(),
            "keywords":       keywords,
            "poster_path":    m.get("poster_path"),
            "backdrop_path":  m.get("backdrop_path"),
            "release_year":   (m.get("release_date") or "")[:4],
            "vote_average":   float(m.get("vote_average", 0)),
            "vote_count":     int(m.get("vote_count", 0)),
            "popularity":     float(m.get("popularity", 0)),
            "runtime":        m.get("runtime"),
            "updated_at":     datetime.utcnow(),
        }])
        print(f"[ensure_movie] Saved full TMDB data for {movie_id}: {title} genres={genres}")
    except Exception as e:
        print(f"[ensure_movie] {movie_id}: {e}")


def _enrich_and_recompute(user_id: str, movie_id: str):
    """Ensure rated movie is in DB with full details, then recompute recs.
    Runs in a background thread after rating submit."""
    _ensure_movie_in_db(movie_id)
    _recompute_recs(user_id)


def _recompute_recs(user_id):
    """Background thread worker — loads all data including implicit signals."""
    try:
        _cache_invalidate(user_id)
        implicit = _load_implicit_signals(user_id)
        recs = compute_recommendations(user_id, _load_all_ratings(), _load_movies_metadata(), implicit=implicit)
        _cache_set(user_id, recs)
        print(f"[worker] {len(recs)} recs for {user_id} (implicit signals: {len(implicit)})")
    except Exception as e:
        print(f"[worker] Error for {user_id}: {e}")


# =============================================================
#  FRANCHISE / UNIVERSE DETECTION
# =============================================================

# Each tuple: (feature_key, list_of_lowercase_markers).
# Markers are tested against a movie's titleLower + keywords + castSearch joined together.
# Order matters — first match sets the primary franchise.
FRANCHISE_SIGNALS = [
    ("franchise:mcu",          ["marvel cinematic universe", "avengers", "iron man",
                                 "captain america", "thor odinson", "black widow",
                                 "spider-man", "guardians of the galaxy", "black panther",
                                 "doctor strange", "ant-man", "thanos"]),
    ("franchise:dc",           ["dc comics", "dc extended universe", "justice league",
                                 "batman", "superman", "wonder woman", "aquaman",
                                 "suicide squad", "gotham city"]),
    ("franchise:star_wars",    ["star wars", "jedi", "sith", "the force",
                                 "galactic empire", "darth vader", "lightsaber"]),
    ("franchise:fast_furious", ["fast & furious", "fast and furious", "dominic toretto"]),
    ("franchise:john_wick",    ["john wick", "the continental hotel"]),
    ("franchise:mi",           ["mission: impossible", "ethan hunt"]),
    ("franchise:james_bond",   ["james bond", "007 spy", "eon productions"]),
    ("franchise:harry_potter", ["harry potter", "hogwarts", "wizarding world",
                                 "voldemort", "fantastic beasts"]),
    ("franchise:lotr",         ["lord of the rings", "middle-earth", "the hobbit",
                                 "frodo baggins", "gandalf", "mordor"]),
    ("franchise:jurassic",     ["jurassic park", "jurassic world"]),
    ("franchise:x_men",        ["x-men", "charles xavier", "magneto", "wolverine"]),
    ("franchise:transformers", ["transformers", "autobots", "decepticons", "optimus prime"]),
    ("franchise:pirates",      ["pirates of the caribbean", "jack sparrow"]),
    ("franchise:indiana_jones",["indiana jones"]),
    ("franchise:monsterverse", ["monsterverse", "mechagodzilla", "godzilla vs"]),
    ("franchise:alien",        ["alien franchise", "xenomorph"]),
    ("franchise:toy_story",    ["toy story", "woody cowboy", "buzz lightyear"]),
    ("franchise:rocky_creed",  ["rocky balboa", "creed boxing"]),
    ("franchise:nolan",        ["christopher nolan"]),
    ("franchise:aamir_khan",   ["aamir khan"]),
    ("franchise:srk",          ["shah rukh khan"]),
    ("franchise:salman_khan",  ["salman khan"]),
    ("franchise:tarantino",    ["quentin tarantino"]),
]

_FRANCHISE_LABELS = {
    "franchise:mcu":          "Marvel Universe",
    "franchise:dc":           "DC Universe",
    "franchise:star_wars":    "Star Wars Universe",
    "franchise:fast_furious": "Fast & Furious series",
    "franchise:john_wick":    "John Wick Universe",
    "franchise:mi":           "Mission: Impossible series",
    "franchise:james_bond":   "James Bond series",
    "franchise:harry_potter": "Wizarding World",
    "franchise:lotr":         "Middle-earth series",
    "franchise:jurassic":     "Jurassic series",
    "franchise:x_men":        "X-Men Universe",
    "franchise:transformers": "Transformers series",
    "franchise:pirates":      "Pirates of the Caribbean",
    "franchise:indiana_jones":"Indiana Jones series",
    "franchise:monsterverse": "Monsterverse",
    "franchise:alien":        "Alien Universe",
    "franchise:toy_story":    "Toy Story series",
    "franchise:rocky_creed":  "Rocky / Creed series",
}


def _detect_franchise(movie):
    """Return list of franchise feature-keys this movie belongs to."""
    text = " ".join(filter(None, [
        movie.get("titleLower") or "",
        " ".join(movie.get("keywords") or []).lower(),
        (movie.get("castSearch") or "").lower(),
    ]))
    return [fkey for fkey, markers in FRANCHISE_SIGNALS if any(m in text for m in markers)]


def _quality_score(movie):
    """0.0–1.0 quality signal: sigmoid on voteAverage (centred 7.0) + log-scaled voteCount."""
    avg   = float(movie.get("voteAverage") or 0)
    count = int(movie.get("voteCount") or 0)
    avg_score   = 1.0 / (1.0 + math.exp(-1.5 * (avg - 7.0)))
    count_score = math.log1p(min(count, 50_000)) / math.log1p(50_000)
    return round(0.6 * avg_score + 0.4 * count_score, 4)


def _passes_quality_gate(movie):
    """Hard filter: exclude movies that are too obscure or poorly rated."""
    return (
        float(movie.get("voteAverage") or 0) >= MIN_VOTE_AVG
        and int(movie.get("voteCount") or 0) >= MIN_VOTE_COUNT
    )


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
    # Franchise / universe membership — highest weight (5.0).
    # Ensures MCU fan sees MCU movies, Star Wars fan sees Star Wars, etc.
    for fkey in _detect_franchise(movie):
        tf[fkey] += 5.0
    # Genre — strong signal (3.0)
    for g in (movie.get("genres") or []):
        if not g:
            continue
        tf[f"genre:{g.lower().replace(' ', '_')}"] += 3.0
    # Lead cast — medium signal (2.0)
    for a in (movie.get("cast") or [])[:5]:
        tf[f"cast:{a.lower().replace(' ', '_')}"] += 2.0
    # Keywords — context signal (1.0)
    for kw in (movie.get("keywords") or [])[:15]:
        tf[f"kw:{kw.lower().replace(' ', '_')}"] += 1.0
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
        # Hard quality gate: don't score junk movies at all
        if not _passes_quality_gate(movie):
            continue
        s = _cosine_similarity(pref, _movie_feature_vector(movie))
        # Minimum similarity threshold — sharing one genre barely qualifies; must be meaningfully similar
        if s >= MIN_CBF_SCORE:
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


def _quality_popular(movies_metadata, exclude_ids: set, top_n: int, reason: str):
    """Return top_n quality movies sampled from a wider quality pool so results vary on each call."""
    pool = [
        m for m in movies_metadata.values()
        if m["movieId"] not in exclude_ids and _passes_quality_gate(m)
    ]
    pool.sort(
        key=lambda m: float(m.get("voteAverage", 0)) * math.log1p(int(m.get("voteCount") or 0)),
        reverse=True,
    )
    # Use a wider bucket (top 3× or 60, whichever is larger) and sample randomly
    # so every page/refresh shows a different slice of quality movies instead of
    # always the same deterministic top-N (Lagaan, ZNMD, Interstellar…)
    bucket = pool[:max(top_n * 3, 60)]
    sample = random.sample(bucket, min(top_n, len(bucket)))
    return _fmt_recs(sample, reason)


def compute_recommendations(user_id, all_ratings, movies_metadata, top_n=TOP_N, implicit=None):
    """
    5-signal hybrid recommendation engine:
      1. Collaborative filtering (CF)       — what similar users liked
      2. Content-based filtering (CBF)      — genres / cast / keywords match
      3. Franchise affinity                 — MCU/DC/LOTR/Bollywood star loyalty
      4. Implicit behaviour signals         — what you viewed / searched / hovered
      5. Quality gating                     — drop junk, boost well-rated films

    Final score = 0.55*relevance + 0.15*quality + 0.15*franchise + 0.15*implicit
    """
    if implicit is None:
        implicit = {}
    user_ratings = all_ratings.get(user_id, {})
    if not user_ratings and not implicit:
        return _quality_popular(movies_metadata, set(), top_n, "Popular on CineCloud")

    # ── Step 1: Build franchise affinity from highly-rated movies ─────────────
    user_franchises: dict = defaultdict(float)
    for mid, rating in user_ratings.items():
        movie = movies_metadata.get(mid)
        if not movie:
            continue
        # Only movies rated 4+ contribute to franchise affinity (like signals)
        if rating >= 4:
            for fkey in _detect_franchise(movie):
                user_franchises[fkey] += (rating - 3.0)
        # Also pull franchise from highly-interacted movies (implicit ≥ 0.5)
    for mid, iscore in implicit.items():
        if iscore >= 0.5:
            movie = movies_metadata.get(mid)
            if movie:
                for fkey in _detect_franchise(movie):
                    user_franchises[fkey] += iscore * 0.5  # implicit counts half of a 4★

    # ── Step 2: Run CF + CBF ───────────────────────────────────────────────────
    cf_norm  = _normalise(_collaborative_filter(user_id, all_ratings))
    cbf_norm = _normalise(_content_based_filter(user_id, all_ratings, movies_metadata))

    # Implicit boosts extra candidates: movies the user interacted with but hasn't rated
    # Expand candidate pool with movies similar to implicitly-viewed ones
    implicit_cbf: dict = defaultdict(float)
    for mid, iscore in implicit.items():
        if mid in user_ratings:
            continue
        ref_movie = movies_metadata.get(mid)
        if not ref_movie or not _passes_quality_gate(ref_movie):
            continue
        ref_vec = _movie_feature_vector(ref_movie)
        for cand_id, cand_movie in movies_metadata.items():
            if cand_id in user_ratings or cand_id == mid:
                continue
            if not _passes_quality_gate(cand_movie):
                continue
            s = _cosine_similarity(ref_vec, _movie_feature_vector(cand_movie))
            if s >= MIN_CBF_SCORE:
                implicit_cbf[cand_id] += s * iscore
    impl_cbf_norm = _normalise(dict(implicit_cbf))
    impl_norm     = _normalise(implicit)

    # Exclude already-rated movies from every signal's candidate set
    candidates = (set(cf_norm) | set(cbf_norm) | set(impl_cbf_norm) | set(impl_norm)) - set(user_ratings)

    if not candidates:
        return _quality_popular(movies_metadata, set(user_ratings), top_n, "Trending Now")

    # ── Step 3: Score every candidate ─────────────────────────────────────────
    scored = []
    for mid in candidates:
        movie = movies_metadata.get(mid)
        if not movie or not _passes_quality_gate(movie):
            continue

        relevance      = cf_norm.get(mid, 0) * CF_WEIGHT + cbf_norm.get(mid, 0) * CBF_WEIGHT
        quality        = _quality_score(movie)
        implicit_score = max(impl_norm.get(mid, 0), impl_cbf_norm.get(mid, 0))

        candidate_franchises = _detect_franchise(movie)
        raw_boost      = sum(user_franchises.get(f, 0) for f in candidate_franchises)
        franchise_norm = min(raw_boost / 5.0, 1.0)

        # Final blend: relevance 55% | quality 15% | franchise 15% | implicit 15%
        final_score = (
            relevance      * 0.55 +
            quality        * 0.15 +
            franchise_norm * 0.15 +
            implicit_score * 0.15
        )

        # ── Reason string ──────────────────────────────────────────────────────
        if candidate_franchises and any(f in user_franchises for f in candidate_franchises):
            matched_f = next(f for f in candidate_franchises if f in user_franchises)
            label     = _FRANCHISE_LABELS.get(matched_f)
            reason    = f"More from the {label}" if label else "More from a franchise you love"
        elif implicit_score > 0.5 and relevance < 0.3:
            reason = "Because you explored similar movies"
        elif cf_norm.get(mid, 0) > cbf_norm.get(mid, 0):
            reason = "Fans of your rated movies loved this"
        else:
            top_genre = next(iter(movie.get("genres") or []), "")
            reason = f"Because you enjoy {top_genre} films" if top_genre else "Matches your taste profile"

        scored.append((final_score, mid, reason, candidate_franchises))

    scored.sort(reverse=True)

    # ── Step 4: Build result list with diversity enforcement ──────────────────
    results = []
    franchise_counts: dict = defaultdict(int)
    seen_title_keys = set()

    for final_score, mid, reason, candidate_franchises in scored[:top_n * 5]:
        if len(results) >= top_n:
            break
        movie = movies_metadata.get(mid)
        if not movie:
            continue

        title_key = (movie.get("titleLower") or "")[:30]
        if title_key in seen_title_keys:
            continue

        if any(franchise_counts[f] >= MAX_PER_FRANCHISE for f in candidate_franchises):
            continue

        for f in candidate_franchises:
            franchise_counts[f] += 1
        seen_title_keys.add(title_key)

        results.append({
            "movieId":     mid,
            "title":       movie.get("title", ""),
            "posterPath":  movie.get("posterPath"),
            "genres":      movie.get("genres", []),
            "voteAverage": float(movie.get("voteAverage", 0)),
            "releaseYear": movie.get("releaseYear", ""),
            "score":       round(final_score, 4),
            "reason":      reason,
        })

    # ── Step 5: Top-up with quality popular only when almost nothing was found ─
    # Keep threshold very low (3) so quality-popular movies don't flood a
    # personalised list just because CBF/CF found fewer than top_n // 2 results.
    if len(results) < 3:
        exclude = set(user_ratings) | {r["movieId"] for r in results}
        results.extend(_quality_popular(movies_metadata, exclude, top_n - len(results), "Trending Now"))

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

