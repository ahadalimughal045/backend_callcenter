import os
import time
from typing import Optional
import jwt
from fastapi import Request
from fastapi.responses import JSONResponse
JWT_SECRET: str = os.getenv("JWT_SECRET", "")
if not JWT_SECRET:
    raise RuntimeError(
        "FATAL: JWT_SECRET environment variable is not set. "
        "The server cannot start without a stable JWT secret. "
        "Set JWT_SECRET in your environment to a secure random string."
    )
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = int(os.getenv("JWT_EXPIRY_SECONDS", str(24 * 60 * 60)))
def create_token(user_id: str, email: str, role: str) -> str:
    now = int(time.time())
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "iat": now,
        "exp": now + JWT_EXPIRY_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
def verify_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None
async def require_auth(request: Request) -> Optional[dict]:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[len("Bearer "):]
    payload = verify_token(token)
    return payload
