import json
import os
from decimal import Decimal
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
MOVIES_TABLE = os.environ["MOVIES_TABLE"]


class DecimalEncoder(json.JSONEncoder):
    """Convert DynamoDB Decimal types to int/float for JSON serialisation."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def _response(status_code: int, body) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Cache-Control": "public, max-age=60",   # CloudFront cache hint
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _qsp(event: dict, key: str, default=None):
    """Safe query string parameter extractor."""
    params = event.get("queryStringParameters") or {}
    return params.get(key, default)


# ─────────────────────────────────────────────────────────────
#  Handlers
# ─────────────────────────────────────────────────────────────

def list_movies(event: dict) -> dict:
    """GET /movies — Paginated movie list, optionally filtered by genre."""
    table = dynamodb.Table(MOVIES_TABLE)
    limit = min(int(_qsp(event, "limit", 20)), 50)
    genre = _qsp(event, "genre")
    last_key_raw = _qsp(event, "nextToken")

    kwargs: dict = {"Limit": limit}

    if last_key_raw:
        try:
            kwargs["ExclusiveStartKey"] = json.loads(
                __import__("base64").b64decode(last_key_raw).decode()
            )
        except Exception:
            pass  # ignore malformed token

    if genre:
        # Query the genre-popularity GSI for filtered browsing
        kwargs["IndexName"] = "genre-popularity-index"
        kwargs["KeyConditionExpression"] = Key("genre").eq(genre)
        result = table.query(**kwargs)
    else:
        result = table.scan(**kwargs)

    movies = result.get("Items", [])
    next_token = None
    if "LastEvaluatedKey" in result:
        next_token = __import__("base64").b64encode(
            json.dumps(result["LastEvaluatedKey"]).encode()
        ).decode()

    return _response(200, {"movies": movies, "count": len(movies), "nextToken": next_token})


def get_movie(event: dict) -> dict:
    """GET /movies/{movieId} — Single movie detail."""
    movie_id = event.get("pathParameters", {}).get("movieId")
    if not movie_id:
        return _response(400, {"error": "movieId is required"})

    table = dynamodb.Table(MOVIES_TABLE)
    result = table.get_item(Key={"movieId": movie_id})
    movie = result.get("Item")

    if not movie:
        return _response(404, {"error": "Movie not found"})

    return _response(200, {"movie": movie})


def search_movies(event: dict) -> dict:
    """GET /movies/search?q=term — Full-text search by title/cast/overview."""
    query = (_qsp(event, "q") or "").strip().lower()
    if len(query) < 2:
        return _response(400, {"error": "Search query must be at least 2 characters"})

    table = dynamodb.Table(MOVIES_TABLE)

    # DynamoDB doesn't have native full-text search, so we use a scan with a
    # FilterExpression. For production, swap this with OpenSearch / Typesense.
    result = table.scan(
        FilterExpression=(
            Attr("titleLower").contains(query)
            | Attr("overviewLower").contains(query)
            | Attr("castSearch").contains(query)
        ),
        Limit=100,
    )

    movies = result.get("Items", [])
    # Sort by popularity descending
    movies.sort(key=lambda m: float(m.get("popularity", 0)), reverse=True)
    return _response(200, {"movies": movies[:20], "count": len(movies[:20])})


def popular_movies(event: dict) -> dict:
    """GET /movies/popular — Top-N movies by popularity score."""
    table = dynamodb.Table(MOVIES_TABLE)
    limit = min(int(_qsp(event, "limit", 20)), 50)

    result = table.scan(
        ProjectionExpression="movieId, title, posterPath, genres, popularity, voteAverage, releaseYear",
        Limit=500,
    )
    movies = result.get("Items", [])
    movies.sort(key=lambda m: float(m.get("popularity", 0)), reverse=True)

    return _response(200, {"movies": movies[:limit], "count": limit})


def list_genres(event: dict) -> dict:
    """GET /movies/genres — Return all distinct genres in the catalog."""
    table = dynamodb.Table(MOVIES_TABLE)
    result = table.scan(
        ProjectionExpression="genres",
        Limit=500,
    )
    genre_set: set = set()
    for item in result.get("Items", []):
        for g in item.get("genres", []):
            genre_set.add(g)

    return _response(200, {"genres": sorted(genre_set)})


# ─────────────────────────────────────────────────────────────
#  Router
# ─────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return _response(200, {})

    if path.endswith("/genres"):
        return list_genres(event)
    if path.endswith("/popular"):
        return popular_movies(event)
    if path.endswith("/search"):
        return search_movies(event)
    if "/movies/" in path and method == "GET":
        return get_movie(event)
    if path.endswith("/movies") and method == "GET":
        return list_movies(event)

    return _response(404, {"error": "Route not found"})
