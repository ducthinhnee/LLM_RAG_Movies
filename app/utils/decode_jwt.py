import os
import jwt

from app.models.response_models import ErrorResponse
from app.utils.exceptions import CustomHTTPException


def decode_jwt(token: str):
    try:
        payload = jwt.decode(
            jwt=token,
            key=os.getenv('SECRET_KEY'),
            algorithms=[os.getenv('ALGORITHM')]
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise CustomHTTPException(
            status_code=403,
            detail=ErrorResponse(
                status=403,
                message="Token has expired"
            ).model_dump()
        )
    except jwt.InvalidTokenError:
        raise CustomHTTPException(
            status_code=403,
            detail=ErrorResponse(
                status=403,
                message="Invalid token"
            ).model_dump()
        )