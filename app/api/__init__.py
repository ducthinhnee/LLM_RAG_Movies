from fastapi import APIRouter
from app.api.knowledge_base import router as knowledge_base_router
from app.api.retriever import router as retriever_router
from app.api.navigation import router as navigation_router
from app.api.rag import router as rag_router
from app.api.create_token import router as create_token_router


router = APIRouter()

router.include_router(
    knowledge_base_router,
    prefix="/knowledge-base",
    tags=["Knowledge Base"]
)

router.include_router(
    retriever_router,
    prefix="/retriever",
    tags=["Retriever"]
)

router.include_router(
    navigation_router,
    prefix="/navigate",
    tags=["AI Navigation"]
)

router.include_router(
    rag_router,
    prefix="/rag",
    tags=["RAG"]
)

router.include_router(
    create_token_router,
    prefix="/create-token",
    tags=["Token"]
)