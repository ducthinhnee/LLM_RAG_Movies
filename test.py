import os
from dotenv import load_dotenv
from langchain_community.embeddings import JinaEmbeddings

# Load variables from .env
load_dotenv()

jina_api_key = os.getenv("JINA_API_KEY")
model = os.getenv("JINA_EMBEDDING", "jina-embeddings-v3")

embeddings = JinaEmbeddings(
    jina_auth_token=jina_api_key,
    model_name=model
)

vector = embeddings.embed_query("Xin chào")
print(f"Dimension: {len(vector)}")
print(f"Vector preview: {vector[:5]}")