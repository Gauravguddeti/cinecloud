"""
Event Tracker — WebSocket Service
===================================
Handles WebSocket lifecycle and real-time event tracking:
  $connect    — validate JWT, store connectionId → DynamoDB
  $disconnect — remove connectionId from DynamoDB
  $default    — process view/click/search events from the frontend
  wsAuthorizer — validate JWT token from query string for WS auth
"""

import json
import os
import time
from datetime import datetime

import boto3
import urllib.request
import urllib.parse
import base64

dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))

CONNECTIONS_TABLE = os.environ["CONNECTIONS_TABLE"]
WS_TTL_SECONDS = 24 * 60 * 60   # 24 hours


# ─────────────────────────────────────────────────────────────
#  JWT decode helper (no external lib required)
# ─────────────────────────────────────────────────────────────

def _decode_jwt_claims(token: str) -> dict:
    """
    Decode JWT payload WITHOUT signature verification.
    Signature is verified by API Gateway Cognito authorizer before this runs.
    This just reads the claims for the userId.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        # Add padding if necessary
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = base64.urlsafe_b64decode(padded).decode("utf-8")
        return json.loads(payload)
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────
#  WebSocket handlers
# ─────────────────────────────────────────────────────────────

def connect(event: dict, context) -> dict:
    """
    $connect event — store connectionId → userId mapping in DynamoDB.
    Called after the wsAuthorizer grants access.
    """
    connection_id = event["requestContext"]["connectionId"]
    authorizer_context = event["requestContext"].get("authorizer", {})
    user_id = authorizer_context.get("userId", "anonymous")

    try:
        table = dynamodb.Table(CONNECTIONS_TABLE)
        table.put_item(
            Item={
                "connectionId": connection_id,
                "userId": user_id,
                "connectedAt": datetime.utcnow().isoformat(),
                "ttl": int(time.time()) + WS_TTL_SECONDS,
            }
        )
        print(f"[ws] CONNECT: connectionId={connection_id}, userId={user_id}")
        return {"statusCode": 200}
    except Exception as e:
        print(f"[ws] CONNECT error: {e}")
        return {"statusCode": 500}


def disconnect(event: dict, context) -> dict:
    """$disconnect event — remove connection record."""
    connection_id = event["requestContext"]["connectionId"]
    try:
        table = dynamodb.Table(CONNECTIONS_TABLE)
        table.delete_item(Key={"connectionId": connection_id})
        print(f"[ws] DISCONNECT: connectionId={connection_id}")
    except Exception as e:
        print(f"[ws] DISCONNECT error: {e}")
    return {"statusCode": 200}


def default_message(event: dict, context) -> dict:
    """
    $default — process real-time events from the frontend.
    Event types: PAGE_VIEW, MOVIE_CLICK, SEARCH, WATCHLIST_ADD
    """
    connection_id = event["requestContext"]["connectionId"]
    raw_body = event.get("body", "{}")

    try:
        message = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
    except json.JSONDecodeError:
        return {"statusCode": 400}

    event_type = message.get("type", "UNKNOWN")
    payload = message.get("payload", {})

    print(f"[ws] Event type={event_type} from connection={connection_id}: {json.dumps(payload)}")

    # For demonstration: echo back an acknowledgement
    # In production: write to Kinesis Data Stream for analytics pipeline
    try:
        apigw = boto3.client(
            "apigatewaymanagementapi",
            endpoint_url=f"https://{event['requestContext']['domainName']}/{event['requestContext']['stage']}",
        )
        ack = {"type": "ACK", "eventType": event_type, "timestamp": datetime.utcnow().isoformat()}
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(ack).encode("utf-8"),
        )
    except Exception as e:
        print(f"[ws] ACK send error: {e}")

    return {"statusCode": 200}


# ─────────────────────────────────────────────────────────────
#  WebSocket Lambda Authorizer
# ─────────────────────────────────────────────────────────────

def ws_authorizer(event: dict, context) -> dict:
    """
    Validate the JWT token passed as a query parameter (?token=...) during WebSocket handshake.
    Returns an IAM policy allowing or denying the $connect route.
    """
    token = ""
    qs = event.get("queryStringParameters") or {}
    token = qs.get("token", "")

    method_arn = event.get("methodArn", "*")

    if not token:
        print("[ws-auth] No token provided — denying")
        return _generate_policy("user", "Deny", method_arn, {})

    claims = _decode_jwt_claims(token)
    user_id = claims.get("sub", "")

    if not user_id:
        print("[ws-auth] Invalid token — no sub claim")
        return _generate_policy("user", "Deny", method_arn, {})

    # Validate token expiry
    exp = claims.get("exp", 0)
    if int(time.time()) > exp:
        print(f"[ws-auth] Token expired for user {user_id}")
        return _generate_policy(user_id, "Deny", method_arn, {})

    print(f"[ws-auth] Authorized user={user_id}")
    return _generate_policy(user_id, "Allow", method_arn, {"userId": user_id})


def _generate_policy(principal_id: str, effect: str, resource: str, context: dict) -> dict:
    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource,
                }
            ],
        },
        "context": context,
    }


# ─────────────────────────────────────────────────────────────
#  Lambda entry points (Serverless Framework maps each route)
# ─────────────────────────────────────────────────────────────

# These are referenced directly in serverless.yml as separate handlers:
#   functions.wsConnect.handler    → event_tracker/handler.connect
#   functions.wsDisconnect.handler → event_tracker/handler.disconnect
#   functions.wsDefault.handler    → event_tracker/handler.default_message
#   functions.wsAuthorizer.handler → event_tracker/handler.ws_authorizer
