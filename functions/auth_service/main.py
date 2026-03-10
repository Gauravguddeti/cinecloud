"""
CineCloud — Auth Service (GCP Cloud Functions)
================================================
Replaces: AWS Lambda + AWS Cognito
Uses:      Firebase Authentication + Firestore (user profiles)

Entry point: http_handler
Deploy:      gcloud functions deploy cinecloud-auth --entry-point=http_handler ...
"""

import json
import os
import urllib.request
import urllib.parse
from datetime import datetime

import firebase_admin
from firebase_admin import auth as firebase_auth, firestore

# ── Singleton init (reused across warm invocations) ───────────
if not firebase_admin._apps:
    firebase_admin.initialize_app()

_db = firestore.client()

FIREBASE_WEB_API_KEY = os.environ.get("FIREBASE_WEB_API_KEY", "")
FIREBASE_IDENTITY_URL = "https://identitytoolkit.googleapis.com/v1/accounts"
FIREBASE_TOKEN_URL = "https://securetoken.googleapis.com/v1/token"

# ── Helpers ───────────────────────────────────────────────────
CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}


def _response(status_code: int, body: dict):
    return (json.dumps(body), status_code, CORS_HEADERS)


def _get_json(request) -> dict:
    try:
        return request.get_json(silent=True) or {}
    except Exception:
        return {}


def _firebase_rest_post(url: str, payload: dict) -> dict:
    """Call Firebase Identity Toolkit REST API."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # nosec — trusted Firebase API
        return json.loads(resp.read().decode())


def _verify_token(request) -> str | None:
    """Extract Firebase ID token from Bearer header, verify, return uid."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None
    token = header.split("Bearer ")[1]
    try:
        decoded = firebase_auth.verify_id_token(token)
        return decoded["uid"]
    except Exception:
        return None


# ── Handlers ──────────────────────────────────────────────────
def register(request) -> tuple:
    """POST /register — Create Firebase Auth user + Firestore profile."""
    body = _get_json(request)
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    name = body.get("name", "").strip()

    if not email or not password:
        return _response(400, {"error": "email and password are required"})
    if len(password) < 8:
        return _response(400, {"error": "password must be at least 8 characters"})

    try:
        # Create Firebase Auth user
        user_record = firebase_auth.create_user(
            email=email,
            password=password,
            display_name=name,
            email_verified=False,
        )
        user_id = user_record.uid

        # Create Firestore user profile
        _db.collection("users").document(user_id).set({
            "userId": user_id,
            "email": email,
            "name": name,
            "preferences": {"genres": [], "languages": ["en"]},
            "createdAt": datetime.utcnow().isoformat(),
            "updatedAt": datetime.utcnow().isoformat(),
            "totalRatings": 0,
        })

        return _response(201, {"message": "User registered successfully", "userId": user_id})

    except firebase_auth.EmailAlreadyExistsError:
        return _response(409, {"error": "An account with this email already exists"})
    except Exception as e:
        print(f"[auth] register error: {e}")
        return _response(500, {"error": "Registration failed. Please try again."})


def login(request) -> tuple:
    """POST /login — Authenticate via Firebase REST API, return JWT tokens."""
    body = _get_json(request)
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")

    if not email or not password:
        return _response(400, {"error": "email and password are required"})

    if not FIREBASE_WEB_API_KEY:
        return _response(500, {"error": "FIREBASE_WEB_API_KEY not configured"})

    try:
        result = _firebase_rest_post(
            f"{FIREBASE_IDENTITY_URL}:signInWithPassword?key={FIREBASE_WEB_API_KEY}",
            {"email": email, "password": password, "returnSecureToken": True},
        )

        user_id = result["localId"]
        id_token = result["idToken"]

        # Fetch Firestore profile
        doc = _db.collection("users").document(user_id).get()
        profile = doc.to_dict() if doc.exists else {}

        return _response(200, {
            "accessToken": id_token,
            "idToken": id_token,
            "refreshToken": result.get("refreshToken"),
            "expiresIn": int(result.get("expiresIn", 3600)),
            "user": {
                "userId": user_id,
                "email": email,
                "name": profile.get("name", ""),
                "preferences": profile.get("preferences", {}),
                "totalRatings": profile.get("totalRatings", 0),
            },
        })

    except urllib.error.HTTPError as e:
        error_body = json.loads(e.read().decode())
        error_msg = error_body.get("error", {}).get("message", "")
        if error_msg in ("EMAIL_NOT_FOUND", "INVALID_PASSWORD", "INVALID_LOGIN_CREDENTIALS"):
            return _response(401, {"error": "Invalid email or password"})
        return _response(401, {"error": "Login failed"})
    except Exception as e:
        print(f"[auth] login error: {e}")
        return _response(500, {"error": "Login failed. Please try again."})


def refresh_token(request) -> tuple:
    """POST /refresh — Exchange a refresh token for a new ID token."""
    body = _get_json(request)
    refresh_tok = body.get("refreshToken", "")

    if not refresh_tok:
        return _response(400, {"error": "refreshToken is required"})
    if not FIREBASE_WEB_API_KEY:
        return _response(500, {"error": "FIREBASE_WEB_API_KEY not configured"})

    try:
        result = _firebase_rest_post(
            f"{FIREBASE_TOKEN_URL}?key={FIREBASE_WEB_API_KEY}",
            {"grant_type": "refresh_token", "refresh_token": refresh_tok},
        )
        return _response(200, {
            "accessToken": result["id_token"],
            "idToken": result["id_token"],
            "expiresIn": int(result.get("expires_in", 3600)),
        })
    except urllib.error.HTTPError:
        return _response(401, {"error": "Refresh token is invalid or expired"})
    except Exception as e:
        print(f"[auth] refresh error: {e}")
        return _response(500, {"error": "Token refresh failed"})


def get_profile(request) -> tuple:
    """GET /profile — Return authenticated user's Firestore profile."""
    user_id = _verify_token(request)
    if not user_id:
        return _response(401, {"error": "Unauthorized — invalid or missing token"})

    doc = _db.collection("users").document(user_id).get()
    if not doc.exists:
        return _response(404, {"error": "Profile not found"})

    profile = doc.to_dict()
    return _response(200, {"user": profile})


# ── Router / Entry Point ──────────────────────────────────────
def http_handler(request):
    """Cloud Function HTTP entry point for all /auth/* routes."""
    if request.method == "OPTIONS":
        return ("", 204, CORS_HEADERS)

    path = request.path.rstrip("/")

    if path.endswith("/register") and request.method == "POST":
        return register(request)
    if path.endswith("/login") and request.method == "POST":
        return login(request)
    if path.endswith("/refresh") and request.method == "POST":
        return refresh_token(request)
    if path.endswith("/profile") and request.method == "GET":
        return get_profile(request)

    return _response(404, {"error": "Route not found"})
