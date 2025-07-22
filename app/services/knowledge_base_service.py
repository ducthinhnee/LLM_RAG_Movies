import uuid
from langchain_core.documents import Document

from app.utils.vector_store import VectorStore


class KnowledgeBaseService:
    @classmethod
    async def add_collection(cls, api_key: str, collection_name: str, documents: [Document]):
        store = VectorStore.get_vector_store(
            api_key=api_key,
            collection_name=collection_name)

        # Safely extract '_id' or generate fallback IDs
        ids = [
            doc.metadata["_id"] if "_id" in doc.metadata else str(uuid.uuid4())
            for doc in documents
        ]
        await store.aadd_documents(documents, ids=ids)

    @classmethod
    async def delete_collection(cls, api_key: str, collection_name: str):
        store = VectorStore.get_vector_store(
            api_key=api_key,
            collection_name=collection_name)
        await store.adelete_collection()

    @classmethod
    async def get_embedded_collection_names(cls):
        return await VectorStore.get_embedded_collection_names()