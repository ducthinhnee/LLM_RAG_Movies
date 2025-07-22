import structlog
from fastapi import APIRouter, Depends

from app.models.request_models import SyncWithAutoRetryRequest, KnowledgeRequest
from app.models.response_models import ErrorResponse, Response
from app.services.knowledge_base_service import KnowledgeBaseService
from app.services.tmdb_service import TMDBService
from app.utils.decode_jwt import decode_jwt
from app.utils.exceptions import CustomHTTPException
from app.utils.transform_document import transform_document

logger = structlog.get_logger(__name__)
router = APIRouter()

async def logic_sync(tmdb_service: TMDBService, llm_api_key: str):
    tmdb_service.connect()

    # Stream data in batches from all collections
    batch_size = 200
    for collection_name, batch in tmdb_service.stream_all_collections_data(batch_size=batch_size):
        print(f"\nCollection: {collection_name}")
        print(f"Batch size: {len(batch)}")

        documents = [transform_document(doc) for doc in batch]
        await KnowledgeBaseService.add_collection(
            api_key=llm_api_key,
            collection_name=collection_name,
            documents=documents
        )

    tmdb_service.close()
    print("Synced successfully")
    return Response(
        status=200,
        data={"message": "Synced successfully"}
    )

@router.post(
    path="/sync",
    tags=["Knowledge Base"],
    description="Sync the knowledge base with the mongodb database")
async def sync(request: KnowledgeRequest = Depends()):
    print("Syncing knowledge base")
    payload = decode_jwt(request.token)
    if payload.get("role") != "admin":
        raise CustomHTTPException(
            status_code=403,
            detail=ErrorResponse(
                status=403,
                message="Access denied"
            ).model_dump()
        )

    tmdb_service = TMDBService()

    try:
        return await logic_sync(tmdb_service, request.llm_api_key)
    except Exception as e:
        tmdb_service.raise_error_sync()
        logger.error("Error syncing knowledge base", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
                status=500,
                message="Error syncing knowledge base").model_dump()
        )
    finally:
        tmdb_service.close()

@router.post(path="/sync-with-auto-retry",
    tags=["Knowledge Base"],
        description="Sync the knowledge base with the mongodb database with auto retry")
async def sync_with_auto_retry(request: SyncWithAutoRetryRequest = Depends()):
    print(f"Syncing knowledge base (Attempt {request.retry_count + 1})")
    payload = decode_jwt(request.token)
    if payload.get("role") != "admin":
        raise CustomHTTPException(
            status_code=403,
            detail=ErrorResponse(
                status=403,
                message="Access denied"
            ).model_dump()
        )

    tmdb_service = TMDBService()

    try:
        return await logic_sync(tmdb_service, request.llm_api_key)
    except Exception as e:
        tmdb_service.raise_error_sync()
        logger.error(f"Error syncing knowledge base (Attempt {request.retry_count + 1})", exc_info=e)

        # Retry logic
        if request.retry_count < request.max_retries:
            return await sync_with_auto_retry(
                SyncWithAutoRetryRequest(
                    llm_api_key= request.llm_api_key,
                    token = request.token,
                    retry_count = request.retry_count + 1,
                    max_retries=request.max_retries
                )
            )
        else:
            raise CustomHTTPException(
                status_code=500,
                detail=ErrorResponse(
                    status=500,
                    message="Error syncing knowledge base after multiple attempts"
                ).model_dump()
            )
    finally:
        tmdb_service.close()

@router.post(
    path="/drop",
    tags=["Knowledge Base"],
    description="Drop the knowledge base")
async def drop(request: KnowledgeRequest = Depends()):
    payload = decode_jwt(request.token)
    if payload.get("role") != "admin":
        raise CustomHTTPException(
            status_code=403,
            detail=ErrorResponse(
                status=403,
                message="Access denied"
            ).model_dump()
        )

    try:
        collection_names = await KnowledgeBaseService.get_embedded_collection_names()

        for collection_name in collection_names:
            print("Dropping collection:", collection_name)
            await KnowledgeBaseService.delete_collection(
                api_key=request.llm_api_key,
                collection_name=collection_name
            )

        return Response(
            status=200,
            data={"message": "All collections dropped successfully"}
        )
    except Exception as e:
        logger.error("Error dropping knowledge base", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
            status=500,
            message="Error dropping knowledge base"
            ).model_dump()
        )

@router.get(
    path="/collections",
    tags=["Knowledge Base"],
    description="List all collections in the knowledge base")
async def list_collections():
    try:
        collection_names = await KnowledgeBaseService.get_embedded_collection_names()
        return Response(
            status=200,
            data={"result": collection_names}
        )
    except Exception as e:
        logger.error("Error listing collections", exc_info=e)
        raise CustomHTTPException(
            status_code=500,
            detail=ErrorResponse(
                status=500,
                message="Error listing collections"
            ).model_dump()
        )