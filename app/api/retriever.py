import structlog
from fastapi import APIRouter, Depends

from app.models.request_models import RetrieverRequest
from app.models.response_models import Response, ErrorResponse
from app.services.retriever_service import RetrieverService
from app.utils.exceptions import CustomHTTPException

logger = structlog.getLogger(__name__)
router = APIRouter()

@router.get(
    path="/",
    tags=["Retriever"],
    description="Search the knowledge base for documents")
async def search(
    request: RetrieverRequest = Depends()
):
    try:
        result = await RetrieverService.search(
            api_key=request.llm_api_key,
            collection_name=request.collection_name,
            query=request.query,
            k=request.amount,
            score_threshold=request.threshold,
        )

        ids = [doc.id for doc in result]
        return Response(
            status=200,
            data={"result": ids}
        )
    except Exception as e:
        logger.error(f"Error searching collection: {request.collection_name} "
                     f"with query: {request.query}", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
                status=500,
                message="Error searching collection"
            ).model_dump()
        )