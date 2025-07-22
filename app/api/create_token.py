import os
import jwt
import structlog
from fastapi import APIRouter
from datetime import datetime, timedelta, timezone

from app.models.response_models import ErrorResponse, Response
from app.utils.exceptions import CustomHTTPException

logger = structlog.get_logger(__name__)
router = APIRouter()

def create_jwt_token(role: str):
    expiration = datetime.now(timezone.utc) + timedelta(hours=2160)  # Token expires in 90 days
    payload = {
        "role": role,
        "exp": expiration,
        "iat": datetime.now(timezone.utc),
    }
    token = jwt.encode(
        payload=payload,
        key=os.getenv('SECRET_KEY'),
        algorithm=os.getenv('ALGORITHM')
    )
    return token

@router.post("/", tags=["Token"], description="Create a JWT token")
def create_token():
    try:
        token = create_jwt_token("admin")
        print("Token: ", token)
        logger.info(f"Token: {token}")
        return Response(
            status=200,
            data={'message': 'Token created successfully'}
        )

    except Exception as e:
        logger.error("Error creating token", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
                status=500,
                message="Error creating token"
            ).model_dump()
        )