import os

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.embeddings import JinaEmbeddings
from pydantic import SecretStr


class ModelService:
    @classmethod
    def get_gemini_embeddings(
            cls,
            google_api_key: str,
            model=os.getenv("GEMINI_EMBEDDING", "models/gemini-embedding-001")
    ):
        return GoogleGenerativeAIEmbeddings(
            model=model,
            google_api_key=SecretStr(google_api_key)
        )

    @classmethod
    def get_openai_embeddings(
            cls,
            openai_api_key: str,
            model=os.getenv("OPENAI_EMBEDDING", "text-embedding-3-large")
    ):
        return OpenAIEmbeddings(
            model=model,
            api_key=SecretStr(openai_api_key)
        )

    @classmethod
    def get_jina_embeddings(
            cls,
            model=os.getenv("JINA_EMBEDDING", "jina-embeddings-v3")
    ):
        jina_api_key = os.getenv("JINA_API_KEY", "")
        return JinaEmbeddings(
            jina_auth_token=jina_api_key,
            model_name=model
        )

    @classmethod
    def get_gemini_model(
            cls,
            google_api_key: str,
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite"),
            temperature=os.getenv("LLM_TEMPERATURE", 0.5)
    ):
        return ChatGoogleGenerativeAI(
            model=model,
            google_api_key=SecretStr(google_api_key),
            temperature=temperature
        )

    @classmethod
    def get_openai_model(
            cls,
            openai_api_key: str,
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=os.getenv("LLM_TEMPERATURE", 0.5)
    ):
        return ChatOpenAI(
            model_name=model,
            api_key=SecretStr(openai_api_key),
            temperature=temperature
        )

    @classmethod
    def get_llm_model(
            cls,
            llm_api_key: str,
            model: str = "",
            temperature: float = 0.5

    ):
        if os.getenv("USE_GEMINI", "True") == "True":
            if model == "":
                return cls.get_gemini_model(llm_api_key, temperature=temperature)
            else:
                return cls.get_gemini_model(llm_api_key, model=model, temperature=temperature)
        else:
            if model == "":
                return cls.get_openai_model(llm_api_key, temperature=temperature)
            else:
                return cls.get_openai_model(llm_api_key, model=model, temperature=temperature)

    @classmethod
    def get_llm_embeddings(cls, llm_api_key: str, model: str = ""):
        if os.getenv("USE_JINA", "False") == "True":
            if model == "":
                return cls.get_jina_embeddings()
            else:
                return cls.get_jina_embeddings(model=model)
        elif os.getenv("USE_GEMINI", "True") == "True":
            if model == "":
                return cls.get_gemini_embeddings(llm_api_key)
            else:
                return cls.get_gemini_embeddings(llm_api_key, model=model)
        else:
            if model == "":
                return cls.get_openai_embeddings(llm_api_key)
            else:
                return cls.get_openai_embeddings(llm_api_key, model=model)