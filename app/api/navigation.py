import structlog
from fastapi import APIRouter, Depends

from app.models.request_models import NavigationRequest
from app.models.response_models import ErrorResponse, Response
from app.services.navigation_service import ai_navigate
from app.utils.exceptions import CustomHTTPException

logger = structlog.get_logger(__name__)
router = APIRouter()

@router.post(
    path="/",
    tags=["AI Navigation"],
    description="Return route and params based on the input query")
async def navigate(request: NavigationRequest = Depends()):
    try:
        result = await ai_navigate(
            llm_api_key=request.llm_api_key,
            question=request.query
        )

        return Response(
            status=200,
            data=result
        )

    except CustomHTTPException as e:
        raise e
    except Exception as e:
        logger.error("Error navigation", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
                status=500,
                message="Call navigation service failed"
            ).
            model_dump()
        )
