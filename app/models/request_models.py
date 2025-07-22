from pydantic import BaseModel, field_validator, Field

class KnowledgeRequest(BaseModel):
    llm_api_key: str = Field(
        ...,
        max_length=250,
        min_length=1,
        description="LLM API key",
        examples=["AIzaSyD1q"])
    token: str = Field(
        ...,
        max_length=250,
        min_length=1,
        description="Admin token to access the endpoint",
        examples=["eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJleH"]
    )

class SyncWithAutoRetryRequest(KnowledgeRequest):
    retry_count: int = Field(
        default=0,
        ge=0,
        description="Retry count",
        examples=[0])
    max_retries: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of retries",
        examples=[50])

    @classmethod
    @field_validator('retry_count')
    def check_retry_count(cls, retry_count, values):
        max_retries = values.get('max_retries')
        if max_retries is not None and retry_count > max_retries:
            raise ValueError(f"retry_count must be less than or equal to max_retries ({max_retries})")
        return retry_count

class AdminRequest(BaseModel):
    llm_api_key: str = Field(
        max_length=250,
        min_length=1,
        description="LLM API key",
        examples=["AIzaSyD1q"]
    )

    token: str = Field(
        max_length=250,
        min_length=1,
        description="Admin token",
        examples=["my_token"]
    )

class RetrieverRequest(BaseModel):
    llm_api_key: str = Field(
        max_length=250,
        min_length=1,
        description="LLM API key",
        examples=["AIzaSyD1q"]
    )
    collection_name: str = Field(
        max_length=50,
        min_length=1,
        description="Collection name",
        examples=["my_collection"]
    )
    query: str = Field(
        max_length=5000,
        min_length=1,
        description="Query string",
        examples=["Suggest scientific films"])
    amount: int = Field(
        default=10,
        ge=2,
        le=100,
        description="Number of results",
        examples=[5])
    threshold: float = Field(
        default=0.25,
        ge=0.0,
        le=1.0,
        description="Threshold for search",
        examples=[0.5]
    )

class RAGRequest(BaseModel):
    llm_api_key: str = Field(
        max_length=250,
        min_length=1,
        description="LLM API key",
        examples=["AIzaSyD1q"]
    )
    collection_name: str = Field(
        max_length=50,
        min_length=1,
        description="Collection name",
        examples=["my_collection"]
    )
    query: str = Field(
        max_length=250,
        min_length=1,
        description="Query string",
        examples=["Suggest scientific films"]
    )

class NavigationRequest(BaseModel):
    llm_api_key: str = Field(
        max_length=250,
        min_length=1,
        description="LLM API key",
        examples=["AIzaSyD1q"]
    )
    query: str = Field(
        max_length=250,
        min_length=1,
        description="Query string",
        examples=["Casts of Moana"]
    )