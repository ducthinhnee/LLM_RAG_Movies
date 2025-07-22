import os
from langchain_postgres.vectorstores import PGVector
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import select, Column, String, UUID, MetaData, Table

from app.services.model_service import ModelService


class VectorStore:
    _connection = (f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
                f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")

    _engine = create_async_engine(
        _connection,
        pool_size=5,  # Number of permanent connections to keep in the pool
        max_overflow=10,  # Number of additional connections allowed beyond pool_size
        pool_timeout=30,  # Number of seconds to wait before giving up on getting a connection from the pool
        pool_recycle=-1,  # Number of seconds after which to recycle a connection
    )

    _collection_metadata = MetaData()
    _langchain_pg_collection = Table('langchain_pg_collection', _collection_metadata,
                                    Column('uuid', UUID, primary_key=True),
                                    Column('name', String))

    @classmethod
    def get_vector_store(cls, api_key: str, collection_name: str) -> PGVector:
        embeddings = ModelService.get_llm_embeddings(llm_api_key=api_key)

        return PGVector(
            embeddings=embeddings,
            collection_name=collection_name,
            connection=cls._engine,
            use_jsonb=True,
            async_mode=True
        )

    @classmethod
    async def get_embedded_collection_names(cls):
        async with AsyncSession(bind=cls._engine) as session:
            async with session.begin():
                # Use the select statement with the defined table
                stmt = select(cls._langchain_pg_collection.c.name)  # Fetch only the 'name' column
                result = await session.execute(stmt)
                records = result.scalars().all()  # Fetch all records as a list
                return records