from pydantic import BaseModel, Field

class Response(BaseModel):
    status: int = Field(
        description="Status code",
        examples=[200]
    )
    data: dict = Field(
        description="Response data",
        examples=[{"result": "my_result"}, {"message": "my_message"}]
    )

class ErrorResponse(BaseModel):
    status: int = Field(description="Status code", examples=[404])
    message: str = Field(description="Error message", examples=["Not found"])
