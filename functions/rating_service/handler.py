"""
Rating Service
POST /ratings  — Submit or update a movie rating (1–5 stars)
GET  /ratings/{userId} — Fetch all ratings for a user

After writing a rating to DynamoDB, publishes a RatingEvent to SQS
so the recommendation worker recomputes recommendations asynchronously.
"""

import json
import os
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
sqs = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))

RATINGS_TABLE = os.environ["RATINGS_TABLE"]
USERS_TABLE = os.environ["USERS_TABLE"]
MOVIES_TABLE = os.environ["MOVIES_TABLE"]
QUEUE_URL = os.environ["RATING_EVENTS_QUEUE_URL"]


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }


def _parse_body(event: dict) -> dict:
    raw = event.get("body") or "{}"
    return json.loads(raw) if isinstance(raw, str) else raw


def _publish_rating_event(user_id: str, movie_id: str, rating: float):
    """Send a message to SQS so the recommendation worker can recompute."""
    message = {
        "eventType": "RATING_SUBMITTED",
        "userId": user_id,
        "movieId": movie_id,
        "rating": rating,
        "timestamp": datetime.utcnow().isoformat(),
    }
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageGroupId=user_id if QUEUE_URL.endswith(".fifo") else None,
        MessageAttributes={
            "eventType": {
                "DataType": "String",
                "StringValue": "RATING_SUBMITTED",
            }
        },
    )


def _publish_rating_event_safe(user_id: str, movie_id: str, rating: float):
    """Fire-and-forget — log error but don't fail the request."""
    try:
        _publish_rating_event(user_id, movie_id, rating)
    except Exception as e:
        print(f"[rating-service] SQS publish failed (non-fatal): {e}")


def submit_rating(event: dict) -> dict:
    """POST /ratings — Create or update a rating."""
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        return _response(401, {"error": "Unauthorized"})

    body = _parse_body(event)
    movie_id = str(body.get("movieId", "")).strip()
    rating_raw = body.get("rating")

    if not movie_id:
        return _response(400, {"error": "movieId is required"})

    # Validate rating value
    try:
        rating = float(rating_raw)
    except (TypeError, ValueError):
        return _response(400, {"error": "rating must be a number between 1 and 5"})

    if not (1 <= rating <= 5):
        return _response(400, {"error": "rating must be between 1 and 5"})

    # Verify movie exists
    movie_table = dynamodb.Table(MOVIES_TABLE)
    movie_result = movie_table.get_item(
        Key={"movieId": movie_id},
        ProjectionExpression="movieId, title",
    )
    if not movie_result.get("Item"):
        return _response(404, {"error": "Movie not found"})

    now = datetime.utcnow().isoformat()
    ratings_table = dynamodb.Table(RATINGS_TABLE)

    # Check if rating already exists (update vs insert)
    existing = ratings_table.get_item(Key={"userId": user_id, "movieId": movie_id}).get("Item")

    ratings_table.put_item(
        Item={
            "userId": user_id,
            "movieId": movie_id,
            "rating": Decimal(str(rating)),
            "title": movie_result["Item"].get("title", ""),
            "createdAt": existing["createdAt"] if existing else now,
            "updatedAt": now,
        }
    )

    # Update user's totalRatings count
    if not existing:
        users_table = dynamodb.Table(USERS_TABLE)
        users_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="ADD totalRatings :inc SET updatedAt = :ts",
            ExpressionAttributeValues={":inc": 1, ":ts": now},
        )

    # Async: trigger recommendation recomputation via SQS
    _publish_rating_event_safe(user_id, movie_id, rating)

    action = "updated" if existing else "created"
    return _response(
        200,
        {
            "message": f"Rating {action} successfully",
            "rating": {"userId": user_id, "movieId": movie_id, "rating": rating},
        },
    )


def get_user_ratings(event: dict) -> dict:
    """GET /ratings/{userId} — Fetch all ratings for the authenticated user."""
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    token_user_id = claims.get("sub")

    path_user_id = (event.get("pathParameters") or {}).get("userId")
    if not path_user_id:
        return _response(400, {"error": "userId is required"})

    # Users can only fetch their own ratings
    if token_user_id != path_user_id:
        return _response(403, {"error": "Forbidden"})

    table = dynamodb.Table(RATINGS_TABLE)
    result = table.query(KeyConditionExpression=Key("userId").eq(path_user_id))
    ratings = result.get("Items", [])

    # Convert Decimal to float for JSON
    for r in ratings:
        r["rating"] = float(r["rating"])

    return _response(200, {"ratings": ratings, "count": len(ratings)})


def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return _response(200, {})

    if method == "POST" and path.endswith("/ratings"):
        return submit_rating(event)

    if method == "GET" and "/ratings/" in path:
        return get_user_ratings(event)

    return _response(404, {"error": "Route not found"})
