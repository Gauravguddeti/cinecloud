"""
ETL Pipeline: Fetch movies from TMDB API → DynamoDB
Triggered by: CloudWatch scheduled event (every Sunday) or manual HTTP POST /admin/ingest
"""

import json
import os
import time
from decimal import Decimal
from datetime import datetime

import boto3
import urllib.request
import urllib.parse

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
MOVIES_TABLE = os.environ["MOVIES_TABLE"]
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body),
    }


def _tmdb_get(path: str, params: dict = None) -> dict:
    """Simple TMDB GET with urllib (no extra dependencies)."""
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    qs = urllib.parse.urlencode(params)
    url = f"{TMDB_BASE}{path}?{qs}"
    with urllib.request.urlopen(url, timeout=10) as resp:  # nosec — TMDB is a trusted API
        return json.loads(resp.read().decode())


def _fetch_genre_map() -> dict:
    """Returns {genre_id: genre_name} mapping."""
    data = _tmdb_get("/genre/movie/list")
    return {g["id"]: g["name"] for g in data.get("genres", [])}


def _transform_movie(raw: dict, genre_map: dict, credits: dict = None) -> dict:
    """Map TMDB movie object → DynamoDB item schema."""
    genre_names = [genre_map.get(gid, "") for gid in raw.get("genre_ids", [])]
    genre_names = [g for g in genre_names if g]

    cast = []
    if credits:
        cast = [m["name"] for m in credits.get("cast", [])[:10]]

    keywords = []
    if credits:
        keywords = [k["name"] for k in credits.get("keywords", {}).get("keywords", [])[:20]]

    title = raw.get("title", "")
    overview = raw.get("overview", "")
    release_year = ""
    rd = raw.get("release_date", "")
    if rd and len(rd) >= 4:
        release_year = rd[:4]

    return {
        "movieId": str(raw["id"]),
        "title": title,
        "titleLower": title.lower(),           # for search
        "overviewLower": overview.lower(),     # for search
        "castSearch": " ".join(cast).lower(),  # for cast search
        "overview": overview,
        "releaseYear": release_year,
        "genres": genre_names,
        "genre": genre_names[0] if genre_names else "Unknown",  # primary genre for GSI
        "cast": cast,
        "keywords": keywords,
        "posterPath": f"{TMDB_IMAGE_BASE}{raw['poster_path']}" if raw.get("poster_path") else None,
        "backdropPath": f"https://image.tmdb.org/t/p/w1280{raw['backdrop_path']}" if raw.get("backdrop_path") else None,
        "popularity": Decimal(str(round(raw.get("popularity", 0), 4))),
        "voteAverage": Decimal(str(round(raw.get("vote_average", 0), 2))),
        "voteCount": int(raw.get("vote_count", 0)),
        "language": raw.get("original_language", "en"),
        "tmdbId": int(raw["id"]),
        "ingestedAt": datetime.utcnow().isoformat(),
    }


def _batch_write(table, items: list):
    """Write items to DynamoDB in batches of 25 (DynamoDB limit)."""
    for i in range(0, len(items), 25):
        batch = items[i:i + 25]
        with table.batch_writer() as writer:
            for item in batch:
                writer.put_item(Item=item)


def ingest(pages: int = 10) -> dict:
    """Fetch `pages` pages of popular movies from TMDB and write to DynamoDB."""
    if not TMDB_API_KEY:
        return {"error": "TMDB_API_KEY environment variable is not set"}

    table = dynamodb.Table(MOVIES_TABLE)
    genre_map = _fetch_genre_map()

    total_ingested = 0
    errors = []

    for page in range(1, pages + 1):
        try:
            data = _tmdb_get("/movie/popular", {"page": page, "language": "en-US"})
            movies_raw = data.get("results", [])
            items = []

            for raw in movies_raw:
                try:
                    # Fetch credits (cast) — 1 extra request per movie
                    # To stay within rate limits, fetch for first 5 pages only
                    credits = None
                    if page <= 5:
                        try:
                            credits = _tmdb_get(f"/movie/{raw['id']}", {"append_to_response": "credits,keywords"})
                        except Exception:
                            pass  # skip credits on error

                    item = _transform_movie(raw, genre_map, credits)
                    items.append(item)
                except Exception as e:
                    errors.append({"movieId": str(raw.get("id")), "error": str(e)})

            _batch_write(table, items)
            total_ingested += len(items)
            print(f"[ETL] page {page}/{pages} — ingested {len(items)} movies (total: {total_ingested})")

            # Respect TMDB rate limit (40 req/10s)
            time.sleep(0.3)

        except Exception as e:
            errors.append({"page": page, "error": str(e)})
            print(f"[ETL] error on page {page}: {e}")

    return {
        "totalIngested": total_ingested,
        "pages": pages,
        "errors": errors[:10],    # cap error list at 10
        "completedAt": datetime.utcnow().isoformat(),
    }


def lambda_handler(event: dict, context) -> dict:
    print(f"[ETL] triggered with event: {json.dumps(event)}")

    # Determine pages to ingest
    pages = 10  # default
    if isinstance(event, dict):
        if "pages" in event:
            pages = int(event["pages"])
        elif event.get("body"):
            try:
                body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
                pages = int(body.get("pages", pages))
            except Exception:
                pass

    pages = max(1, min(pages, 50))  # clamp to 1–50
    result = ingest(pages)

    # If called via HTTP, return HTTP response
    if event.get("httpMethod"):
        return _response(200, result)

    # If called via schedule, just return the result dict
    return result
