"""
Recommendation Worker — SQS Consumer
=====================================
Triggered by SQS messages published by the rating-service.
Asynchronously recomputes recommendations for the user who just rated a movie,
then pushes the new recommendations to all active WebSocket connections for that user.

This is the "event-driven" core of the architecture.
"""

import json
import os
import time

import boto3

from handler import (
    _cache_invalidate,
    _cache_set,
    _load_all_ratings,
    _load_movies_metadata,
    compute_recommendations,
)

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))
api_gw = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=os.environ.get("WEBSOCKET_API_ENDPOINT", "http://localhost:4001"),
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
)

CONNECTIONS_TABLE = os.environ["CONNECTIONS_TABLE"]


def _get_user_connections(user_id: str) -> list[str]:
    """Return all active WebSocket connection IDs for a user."""
    try:
        table = dynamodb.Table(CONNECTIONS_TABLE)
        result = table.query(
            IndexName="userId-index",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("userId").eq(user_id),
        )
        return [item["connectionId"] for item in result.get("Items", [])]
    except Exception as e:
        print(f"[worker] Failed to fetch connections for {user_id}: {e}")
        return []


def _push_to_websocket(connection_id: str, payload: dict):
    """Send a message to a WebSocket connection."""
    try:
        api_gw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload).encode("utf-8"),
        )
    except api_gw.exceptions.GoneException:
        # Connection is stale — clean it up
        try:
            dynamodb.Table(CONNECTIONS_TABLE).delete_item(
                Key={"connectionId": connection_id}
            )
        except Exception:
            pass
    except Exception as e:
        print(f"[worker] WebSocket push failed for {connection_id}: {e}")


def _process_rating_event(user_id: str, movie_id: str, rating: float):
    """Core logic: recompute recommendations and push to WebSocket."""
    print(f"[worker] Recomputing recommendations for user={user_id} after rating movie={movie_id}")
    t0 = time.time()

    # Invalidate stale cache
    _cache_invalidate(user_id)

    # Recompute
    all_ratings = _load_all_ratings()
    movies_metadata = _load_movies_metadata()
    recs = compute_recommendations(user_id, all_ratings, movies_metadata)

    # Cache the new result
    _cache_set(user_id, recs)

    elapsed = round(time.time() - t0, 3)
    print(f"[worker] Recomputation done in {elapsed}s — {len(recs)} recommendations")

    # Push to all active WebSocket connections for this user
    connections = _get_user_connections(user_id)
    if connections:
        push_payload = {
            "type": "RECOMMENDATIONS_UPDATED",
            "userId": user_id,
            "recommendations": recs[:10],   # send top 10 over WS
            "computeTimeSeconds": elapsed,
            "triggeredBy": {"movieId": movie_id, "rating": rating},
        }
        for conn_id in connections:
            _push_to_websocket(conn_id, push_payload)
        print(f"[worker] Pushed to {len(connections)} WebSocket connection(s)")


def lambda_handler(event: dict, context) -> dict:
    """
    SQS batch handler.
    Returns ReportBatchItemFailures format so only failed messages are retried.
    """
    failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            body = json.loads(record["body"])
            user_id = body.get("userId")
            movie_id = body.get("movieId")
            rating = float(body.get("rating", 0))

            if not user_id or not movie_id:
                print(f"[worker] Skipping malformed message {message_id}")
                continue

            _process_rating_event(user_id, movie_id, rating)

        except Exception as e:
            print(f"[worker] ERROR processing message {message_id}: {e}")
            failures.append({"itemIdentifier": message_id})

    # ReportBatchItemFailures — only failed messages go back to the queue
    return {"batchItemFailures": failures}
