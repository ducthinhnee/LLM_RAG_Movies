from langchain import hub
from langchain_core.documents import Document
from langchain_core.messages import trim_messages
from langgraph.graph import START, StateGraph
from typing_extensions import List, TypedDict

from app.services.model_service import ModelService
from app.utils.vector_store import VectorStore

# Define prompt for question-answering
prompt = hub.pull("rlm/rag-prompt")

def build_rag_graph(api_key: str, collection_name: str):
    # Define state for application
    class State(TypedDict):
        question: str
        context: List[Document]
        answer: str

    # Define application steps
    async def retrieve(state: State):
        vector_store = VectorStore.get_vector_store(
            api_key=api_key,
            collection_name=collection_name
        )
        retrieved_docs = await vector_store.asimilarity_search(state["question"])
        return {"context": retrieved_docs}


    async def generate(state: State):
        docs_content = "\n\n".join(doc.page_content for doc in state["context"])
        messages = prompt.invoke({"question": state["question"], "context": docs_content})
        model = ModelService.get_llm_model(llm_api_key=api_key)
        trim_messages(
            messages,
            max_tokens=3000,
            strategy="last",
            token_counter=model,
            include_system=True,
            allow_partial=True,
        )
        response = await model.ainvoke(messages)
        return {"answer": response.content}

    # Compile application and test
    graph_builder = StateGraph(State).add_sequence([retrieve, generate])
    graph_builder.add_edge(START, "retrieve")
    graph = graph_builder.compile()
    return graph

async def ask_question(api_key: str, collection_name: str, question: str):
    graph = build_rag_graph(api_key, collection_name)
    response = await graph.ainvoke({"question": question})
    return response["answer"]