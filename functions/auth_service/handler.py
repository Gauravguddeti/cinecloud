import json
import os
import boto3
import hmac
import hashlib
import base64
from datetime import datetime

cognito = boto3.client("cognito-idp", region_name=os.environ.get("AWS_REGION", "us-east-1"))
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))

CLIENT_ID = os.environ["COGNITO_CLIENT_ID"]
USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]
USERS_TABLE = os.environ["USERS_TABLE"]


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body),
    }


def _parse_body(event: dict) -> dict:
    raw = event.get("body") or "{}"
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def register(event: dict) -> dict:
    """POST /auth/register — Create Cognito user + DynamoDB profile."""
    body = _parse_body(event)
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    name = body.get("name", "").strip()

    if not email or not password:
        return _response(400, {"error": "email and password are required"})

    if len(password) < 8:
        return _response(400, {"error": "password must be at least 8 characters"})

    try:
        # 1. Create Cognito user
        cognito.sign_up(
            ClientId=CLIENT_ID,
            Username=email,
            Password=password,
            UserAttributes=[
                {"Name": "email", "Value": email},
                {"Name": "name", "Value": name},
            ],
        )

        # 2. Auto-confirm for demo purposes  (in production: require email verification)
        cognito.admin_confirm_sign_up(UserPoolId=USER_POOL_ID, Username=email)

        # 3. Get Cognito sub (userId)
        cognito_user = cognito.admin_get_user(UserPoolId=USER_POOL_ID, Username=email)
        user_id = next(
            attr["Value"]
            for attr in cognito_user["UserAttributes"]
            if attr["Name"] == "sub"
        )

        # 4. Create user profile in DynamoDB
        table = dynamodb.Table(USERS_TABLE)
        table.put_item(
            Item={
                "userId": user_id,
                "email": email,
                "name": name,
                "preferences": {"genres": [], "languages": ["en"]},
                "createdAt": datetime.utcnow().isoformat(),
                "updatedAt": datetime.utcnow().isoformat(),
                "totalRatings": 0,
            }
        )

        return _response(201, {"message": "User registered successfully", "userId": user_id})

    except cognito.exceptions.UsernameExistsException:
        return _response(409, {"error": "An account with this email already exists"})
    except cognito.exceptions.InvalidPasswordException as e:
        return _response(400, {"error": str(e)})
    except Exception as e:
        print(f"[register] error: {e}")
        return _response(500, {"error": "Registration failed. Please try again."})


def login(event: dict) -> dict:
    """POST /auth/login — Authenticate and return JWT tokens."""
    body = _parse_body(event)
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")

    if not email or not password:
        return _response(400, {"error": "email and password are required"})

    try:
        auth_resp = cognito.initiate_auth(
            ClientId=CLIENT_ID,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": email, "PASSWORD": password},
        )
        result = auth_resp["AuthenticationResult"]

        # Fetch user profile from DynamoDB
        cognito_user = cognito.get_user(AccessToken=result["AccessToken"])
        user_id = next(
            attr["Value"]
            for attr in cognito_user["UserAttributes"]
            if attr["Name"] == "sub"
        )

        table = dynamodb.Table(USERS_TABLE)
        profile = table.get_item(Key={"userId": user_id}).get("Item", {})

        return _response(
            200,
            {
                "accessToken": result["AccessToken"],
                "idToken": result["IdToken"],
                "refreshToken": result["RefreshToken"],
                "expiresIn": result["ExpiresIn"],
                "user": {
                    "userId": user_id,
                    "email": email,
                    "name": profile.get("name", ""),
                    "preferences": profile.get("preferences", {}),
                    "totalRatings": profile.get("totalRatings", 0),
                },
            },
        )

    except cognito.exceptions.NotAuthorizedException:
        return _response(401, {"error": "Invalid email or password"})
    except cognito.exceptions.UserNotFoundException:
        return _response(401, {"error": "Invalid email or password"})
    except Exception as e:
        print(f"[login] error: {e}")
        return _response(500, {"error": "Login failed. Please try again."})


def refresh_token(event: dict) -> dict:
    """POST /auth/refresh — Refresh access token."""
    body = _parse_body(event)
    refresh_token_val = body.get("refreshToken", "")

    if not refresh_token_val:
        return _response(400, {"error": "refreshToken is required"})

    try:
        auth_resp = cognito.initiate_auth(
            ClientId=CLIENT_ID,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token_val},
        )
        result = auth_resp["AuthenticationResult"]
        return _response(
            200,
            {
                "accessToken": result["AccessToken"],
                "idToken": result["IdToken"],
                "expiresIn": result["ExpiresIn"],
            },
        )
    except cognito.exceptions.NotAuthorizedException:
        return _response(401, {"error": "Refresh token is invalid or expired"})
    except Exception as e:
        print(f"[refresh_token] error: {e}")
        return _response(500, {"error": "Token refresh failed"})


def get_profile(event: dict) -> dict:
    """GET /auth/profile — Return authenticated user's profile."""
    try:
        claims = event["requestContext"]["authorizer"]["claims"]
        user_id = claims["sub"]

        table = dynamodb.Table(USERS_TABLE)
        result = table.get_item(Key={"userId": user_id})
        profile = result.get("Item")

        if not profile:
            return _response(404, {"error": "Profile not found"})

        # Remove sensitive fields
        profile.pop("passwordHash", None)
        return _response(200, {"user": profile})

    except Exception as e:
        print(f"[get_profile] error: {e}")
        return _response(500, {"error": "Failed to fetch profile"})


def lambda_handler(event: dict, context) -> dict:
    """Route HTTP events to the correct handler."""
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return _response(200, {})

    if path.endswith("/register") and method == "POST":
        return register(event)
    if path.endswith("/login") and method == "POST":
        return login(event)
    if path.endswith("/refresh") and method == "POST":
        return refresh_token(event)
    if path.endswith("/profile") and method == "GET":
        return get_profile(event)

    return _response(404, {"error": "Route not found"})
