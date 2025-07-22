import os
import structlog
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.api import router
from app.models.response_models import ErrorResponse, Response
from app.utils.exceptions import CustomHTTPException

logger = structlog.get_logger(__name__)
app = FastAPI(
    debug=os.getenv("DEBUG", False),
    title="Action-Executing AI Service API",
    description="This is a AI SERVICE API using FastAPI and Swagger UI.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": ErrorResponse(
                status=500,
                message="An internal server error occurred"
            ).model_dump()
        }
    )

@app.exception_handler(CustomHTTPException)
async def my_custom_exception_handler(request: Request, exc: CustomHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.detail,
    )

@app.get("/healthy", tags=["Health Check"], description="Check the health of the service")
def healthy():
    return Response(
        status=200,
        data={"message": "Service is healthy"}
    )

app.include_router(router)