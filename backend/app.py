"""
CineCloud — Unified Flask Backend for Render.com
=================================================
Replaces: 7 GCP Cloud Functions (no billing required!)
Uses:     Firebase Auth + Firestore + Upstash Redis
Deploy:   render.com free tier (see render.yaml)

Keep-alive: self-pings /ping every 13 min so Render free tier never sleeps
Real-time:  Background thread writes updated recs to Firestore
            → frontend Firestore onSnapshot fires automatically ⚡
"""

import json
import math
import os
import threading
import time
from collections import defaultdict
from datetime import datetime

import firebase_admin
import requests
from firebase_admin import credentials, auth as firebase_auth, firestore
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    import redis as redis_lib
    _redis_available = True
except ImportError:
    _redis_available = False

# ── Firebase Admin init ───────────────────────────────────────
# On Render: set FIREBASE_SERVICE_ACCOUNT_JSON env var (JSON string)
# Locally:   uses Application Default Credentials (gcloud auth)
if not firebase_admin._apps:
    _sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if _sa_json:
        cred = credentials.Certificate(json.loads(_sa_json))
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()

_db = firestore.client()

# ── Config ────────────────────────────────────────────────────
REDIS_URL            = os.environ.get("REDIS_URL", "")
FIREBASE_WEB_API_KEY = os.environ.get("FIREBASE_WEB_API_KEY", "")
TMDB_API_KEY         = os.environ.get("TMDB_API_KEY", "")
TOP_N                = 20
CF_WEIGHT            = 0.70
CBF_WEIGHT           = 0.30
MIN_RATINGS_FOR_CF   = 3
CACHE_TTL_REDIS      = 1800    # 30 min
CACHE_TTL_FIRESTORE  = 86400   # 24 h

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


# ── Keep-alive thread (prevents Render free tier sleep) ───────
def _keep_alive_loop():
    """Self-ping every 14 min as backup. QStash is the primary keep-alive (every 5 min)."""
    self_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not self_url:
        print("[keep-alive] RENDER_EXTERNAL_URL not set — skipping")
        return
    print(f"[keep-alive] Backup self-ping every 14 min ({self_url}/ping)")
    while True:
        time.sleep(14 * 60)
        try:
            requests.get(f"{self_url}/ping", timeout=10)
            print(f"[keep-alive] Pinged self at {datetime.utcnow().isoformat()}")
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


# ── Auth helper ───────────────────────────────────────────────
def _verify_token():
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    try:
        return firebase_auth.verify_id_token(header.split("Bearer ")[1])["uid"]
    except Exception:
        return None


def _unauth():
    return jsonify({"error": "Unauthorized"}), 401


# =============================================================
#  AUTH
# =============================================================

@app.route("/auth/register", methods=["POST"])
def auth_register():
    body = request.get_json() or {}
    email    = body.get("email", "").strip()
    password = body.get("password", "")
    name     = body.get("name", "").strip()
    if not email or not password or not name:
        return jsonify({"error": "email, password and name required"}), 400
    try:
        user = firebase_auth.create_user(email=email, password=password, display_name=name)
    except firebase_auth.EmailAlreadyExistsError:
        return jsonify({"error": "Email already registered"}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    _db.collection("users").document(user.uid).set({
        "userId": user.uid, "email": email, "name": name,
        "createdAt": datetime.utcnow().isoformat(),
    })
    return jsonify({"message": "User created", "userId": user.uid}), 201


@app.route("/auth/login", methods=["POST"])
def auth_login():
    body  = request.get_json() or {}
    email = body.get("email", "")
    pw    = body.get("password", "")
    if not FIREBASE_WEB_API_KEY:
        return jsonify({"error": "Server misconfigured"}), 500

    resp = requests.post(
        f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={FIREBASE_WEB_API_KEY}",
        json={"email": email, "password": pw, "returnSecureToken": True},
        timeout=10,
    )
    data = resp.json()
    if not resp.ok:
        msg = data.get("error", {}).get("message", "Login failed")
        return jsonify({"error": msg}), 401

    uid     = data["localId"]
    profile = _db.collection("users").document(uid).get()
    user    = profile.to_dict() if profile.exists else {"userId": uid, "email": email, "name": ""}
    return jsonify({
        "accessToken":  data["idToken"],
        "idToken":      data["idToken"],
        "refreshToken": data["refreshToken"],
        "user":         user,
    })


@app.route("/auth/profile", methods=["GET"])
def auth_profile():
    uid = _verify_token()
    if not uid:
        return _unauth()
    doc = _db.collection("users").document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": doc.to_dict()})


# =============================================================
#  MOVIES
# =============================================================

@app.route("/movies/list", methods=["GET"])
def movies_list():
    genre = request.args.get("genre")
    limit = min(int(request.args.get("limit", 50)), 200)
    query = _db.collection("movies")
    if genre:
        query = query.where(filter=firestore.FieldFilter("genres", "array_contains", genre))
    docs   = query.limit(limit).stream()
    movies = [d.to_dict() for d in docs]
    return jsonify({"movies": movies, "count": len(movies)})


@app.route("/movies/search", methods=["GET"])
def movies_search():
    q = request.args.get("q", "").lower().strip()
    if not q:
        return jsonify({"movies": [], "count": 0})
    docs    = _db.collection("movies").limit(300).stream()
    results = [
        d.to_dict() for d in docs
        if q in d.to_dict().get("titleLower", "")
        or q in d.to_dict().get("overviewLower", "")
        or q in d.to_dict().get("castSearch", "")
    ]
    return jsonify({"movies": results, "count": len(results)})


@app.route("/movies/popular", methods=["GET"])
def movies_popular():
    limit = min(int(request.args.get("limit", 20)), 100)
    docs  = (
        _db.collection("movies")
           .order_by("popularity", direction=firestore.Query.DESCENDING)
           .limit(limit)
           .stream()
    )
    movies = [d.to_dict() for d in docs]
    return jsonify({"movies": movies, "count": len(movies)})


@app.route("/movies/genres", methods=["GET"])
def movies_genres():
    genres = set()
    for d in _db.collection("movies").limit(500).stream():
        genres.update(d.to_dict().get("genres", []))
    return jsonify({"genres": sorted(genres)})


@app.route("/movies/detail/<movie_id>", methods=["GET"])
def movies_detail(movie_id):
    doc = _db.collection("movies").document(movie_id).get()
    if not doc.exists:
        return jsonify({"error": "Movie not found"}), 404
    return jsonify({"movie": doc.to_dict()})


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

    now  = datetime.utcnow().isoformat()
    data = {"userId": uid, "movieId": movie_id, "rating": rating,
            "ratedAt": now, "updatedAt": now}
    _db.collection("ratings").document(f"{uid}_{movie_id}").set(data)
    _db.collection("movies").document(movie_id).set(
        {"totalRatings": firestore.Increment(1)}, merge=True
    )

    # Async recompute — replaces Pub/Sub worker ⚡
    threading.Thread(target=_recompute_recs, args=(uid,), daemon=True).start()
    return jsonify({"message": "Rating submitted", "rating": data})


@app.route("/ratings/user/<user_id>", methods=["GET"])
def ratings_get_user(user_id):
    uid = _verify_token()
    if not uid or uid != user_id:
        return _unauth()
    docs = (
        _db.collection("ratings")
           .where(filter=firestore.FieldFilter("userId", "==", user_id))
           .stream()
    )
    ratings = [d.to_dict() for d in docs]
    return jsonify({"ratings": ratings, "count": len(ratings)})


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
    t0             = time.time()
    all_ratings    = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs           = compute_recommendations(user_id, all_ratings, movies_metadata)
    elapsed        = round(time.time() - t0, 3)
    _cache_set(user_id, recs)
    return jsonify({"recommendations": recs, "userId": user_id,
                    "fromCache": False, "computeTimeSeconds": elapsed,
                    "count": len(recs)})


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

@app.route("/admin/ingest", methods=["POST"])
def admin_ingest():
    body  = request.get_json() or {}
    pages = min(int(body.get("pages", 3)), 20)
    if not TMDB_API_KEY:
        return jsonify({"error": "TMDB_API_KEY not set"}), 500

    batch = []
    total = 0
    for page in range(1, pages + 1):
        resp = requests.get(
            "https://api.themoviedb.org/3/movie/popular",
            params={"api_key": TMDB_API_KEY, "page": page, "language": "en-US"},
            timeout=15,
        )
        if not resp.ok:
            continue
        for movie in resp.json().get("results", []):
            detail = requests.get(
                f"https://api.themoviedb.org/3/movie/{movie['id']}",
                params={"api_key": TMDB_API_KEY, "append_to_response": "credits,keywords"},
                timeout=15,
            )
            if not detail.ok:
                continue
            d       = detail.json()
            genres  = [g["name"] for g in d.get("genres", [])]
            cast    = [c["name"] for c in d.get("credits", {}).get("cast", [])[:10]]
            kws     = [k["name"] for k in d.get("keywords", {}).get("keywords", [])[:20]]
            batch.append({
                "movieId":       str(d["id"]),
                "title":         d.get("title", ""),
                "titleLower":    d.get("title", "").lower(),
                "overview":      d.get("overview", ""),
                "overviewLower": d.get("overview", "").lower(),
                "genres":        genres,
                "cast":          cast,
                "castSearch":    " ".join(cast).lower(),
                "keywords":      kws,
                "posterPath":    d.get("poster_path"),
                "backdropPath":  d.get("backdrop_path"),
                "releaseYear":   (d.get("release_date") or "")[:4],
                "voteAverage":   float(d.get("vote_average", 0)),
                "voteCount":     int(d.get("vote_count", 0)),
                "popularity":    float(d.get("popularity", 0)),
                "runtime":       d.get("runtime"),
                "updatedAt":     datetime.utcnow().isoformat(),
            })
            if len(batch) == 500:
                _write_firestore_batch(batch)
                total += len(batch)
                batch = []

    if batch:
        _write_firestore_batch(batch)
        total += len(batch)
    return jsonify({"message": f"Ingested {total} movies", "total": total})


def _write_firestore_batch(movies):
    wb = _db.batch()
    for m in movies:
        ref = _db.collection("movies").document(m["movieId"])
        wb.set(ref, m, merge=True)
    wb.commit()


# =============================================================
#  EVENTS & HEALTH
# =============================================================

@app.route("/events/track", methods=["POST"])
def events_track():
    uid = _verify_token()
    if not uid:
        return _unauth()
    body = request.get_json() or {}
    _db.collection("events").add({
        "userId":     uid,
        "eventType":  body.get("eventType", ""),
        "properties": body.get("properties", {}),
        "timestamp":  datetime.utcnow().isoformat(),
    })
    return jsonify({"tracked": True})


@app.route("/ping", methods=["GET", "POST"])
def ping():
    return jsonify({"status": "ok", "ts": int(time.time())})


@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "CineCloud API", "status": "running",
                    "version": "2.0-render"})


# =============================================================
#  CACHE HELPERS
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
    try:
        doc = _db.collection("recommendations").document(user_id).get()
        if doc.exists:
            data = doc.to_dict()
            if int(data.get("ttl", 0)) > int(time.time()):
                return data.get("recommendations", [])
    except Exception:
        pass
    return None


def _cache_set(user_id, recs):
    r = _get_redis()
    if r:
        try:
            r.setex(f"rec:{user_id}", CACHE_TTL_REDIS, json.dumps(recs))
        except Exception:
            pass
    try:
        _db.collection("recommendations").document(user_id).set({
            "userId":          user_id,
            "recommendations": recs,
            "computedAt":      datetime.utcnow().isoformat(),
            "ttl":             int(time.time()) + CACHE_TTL_FIRESTORE,
        })
    except Exception as e:
        print(f"[cache] Firestore write failed: {e}")


def _cache_invalidate(user_id):
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


# =============================================================
#  DATA LOADERS (Firestore)
# =============================================================

def _load_all_ratings():
    ratings = defaultdict(dict)
    for doc in _db.collection("ratings").stream():
        r = doc.to_dict()
        ratings[r["userId"]][r["movieId"]] = float(r["rating"])
    return dict(ratings)


def _load_movies_metadata():
    movies = {}
    for doc in _db.collection("movies").stream():
        data          = doc.to_dict()
        movies[data["movieId"]] = data
    return movies


# =============================================================
#  RECOMMENDATION ALGORITHM  — Hybrid CF (70%) + CBF (30%)
# =============================================================

def _recompute_recs(user_id):
    """Background thread worker — replaces GCP Pub/Sub consumer."""
    try:
        _cache_invalidate(user_id)
        recs = compute_recommendations(user_id, _load_all_ratings(), _load_movies_metadata())
        _cache_set(user_id, recs)
        print(f"[worker] {len(recs)} recs for {user_id} written to Firestore")
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
        tf[f"genre:{g.lower().replace(' ','_')}"] += 3.0
    for a in movie.get("cast", [])[:5]:
        tf[f"cast:{a.lower().replace(' ','_')}"]  += 2.0
    for kw in movie.get("keywords", [])[:15]:
        tf[f"kw:{kw.lower().replace(' ','_')}"]   += 1.0
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
