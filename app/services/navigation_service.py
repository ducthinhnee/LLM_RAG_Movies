import json
import os
import structlog
from enum import Enum

from langgraph.constants import END
from langgraph.graph import START, StateGraph
from pymongo import MongoClient
from typing_extensions import TypedDict

from app.services.model_service import ModelService

logger = structlog.getLogger(__name__)

class RouteEnum(Enum):
    HOME_PAGE = "HOME_PAGE"
    PROFILE_PAGE = "PROFILE_PAGE"
    SEARCH_PAGE = "SEARCH_PAGE"
    CAST_PAGE = "CAST_PAGE"
    MOVIE_PAGE = "MOVIE_PAGE"
    GENRE_PAGE = "GENRE_PAGE"
    NONE = "NONE"

MAX_ROWS = 5

def build_graph(llm_api_key: str):
    class State(TypedDict):
        question: str
        route: RouteEnum
        params: dict
        metadata: str
        is_success: bool

    # 1. Select route based on user's need
    async def select_route(state: State):
        llm = ModelService.get_llm_model(llm_api_key=llm_api_key)
        template = (
            "You are an assistant bot helping users choose the correct route for navigation.\n"
            f"Based on the user's need: '{state['question']}', your task is to return only one route name that the user should navigate to:\n"
            "The available routes are:\n"
            "- **CAST_PAGE**: User wants to view the cast members or actors list of a movie.\n"
            "- **HOME_PAGE**: User wants to return to the home page.\n"
            "- **MOVIE_PAGE**: User wants to view detailed information about a movie.\n"
            "- **SEARCH_PAGE**: User wants to go to the search page.\n"
            "- **GENRE_PAGE**: User wants to explore movies in a specific genre or category.\n"
            "- **PROFILE_PAGE**: User wants to view their profile.\n"
            "- **NONE**: If you can't determine the appropriate route.\n"
            "Conditions:\n"
            "- **SEARCH_PAGE**: Only return this route if the user's question contains words like 'search' or 'keyword'.\n"
            "  For example: 'Search with keyword X' or 'search for movies'.\n"
            "- In all other cases, return the most relevant route based on the user's need. Only return the raw-text of route name, with no specific format.\n"
        )

        response = await llm.ainvoke(template)
        print(response.content)
        route_value = response.content.strip().replace("*", "")
        route = RouteEnum(route_value) if route_value in RouteEnum._value2member_map_ else RouteEnum.NONE
        return {"route": route}


    # 2. Generate parameters based on the selected route
    async def generate_and_query(state: State):
        template_movie = (
            f"You are an assistant that translates user queries into MongoDB queries.\n"
            f"Collection details:\n"
            f"- `movies`: Fields include id, title, original_title, credits.cast.name, credits.crew.name, genres.name, budget, revenue, release_date, vote_average, imdb_id, production_companies.name, production_countries.name, overview, tagline.\n\n"
            f"**Task**:\n"
            f"1. Identify the collection to query (`collection`).\n"
            f"2. Create a MongoDB query (`query`) as a Python dictionary.\n\n"
            f"**Rules**:\n"
            f"- Use `$regex` for text fields to allow partial, case-insensitive matches (`$options: 'i'`).\n"
            f"- Combine conditions with `$or` if multiple fields are relevant.\n"
            f"- Return `'collection': None, 'query': None` if no valid query can be created.\n\n"
            f"**Examples**:\n"
            f"- Query: 'Movie The Dark Knight.'\n"
            f"  Output: {{'collection': 'movies', 'query': {{'title': 'The Dark Knight'}}}}\n"
            f"- Query: 'Casts of Moana'\n"
            f"  Output: {{'collection': 'movies', 'query': {{'title': {{'$regex': 'Moana', '$options': 'i'}}}}}}\n"
            f"- Query: 'Superheroes film'\n"
            f"  Output: {{'collection':'movies','query':{{'$or':[{{'title':{{'$regex':'Superheroes','$options':'i'}}}},{{'credits.cast.name':{{'$regex':'Superheroes','$options':'i'}}}},{{'overview':{{'$regex':'Superheroes','$options':'i'}}}}]}}}}\n"
            f"- Query: 'Tell me about weather updates.'\n"
            f"  Output: {{'collection': None, 'query': None}}\n\n"
            f"Process this user query: '{state['question']}' and just return only a one-line text JSON result."
        )

        template_genre = (
            f"You are an assistant that translates user queries into MongoDB queries.\n"
            f"Collection details:\n"
            f"- `movie_genres`: Fields include name, id.\n\n"
            f"**Task**:\n"
            f"1. Identify the collection to query (`collection`).\n"
            f"2. Create a MongoDB query (`query`) as a Python dictionary.\n\n"
            f"**Rules**:\n"
            f"- Use `$regex` for text fields like `name` to allow partial, case-insensitive matches (`$options: 'i'`).\n"
            f"- Return `'collection': None, 'query': None` if no valid query can be created.\n\n"
            f"**Examples**:\n"
            f"- Query: 'Find genres related to Action.'\n"
            f"  Output: {{'collection': 'movie_genres', 'query': {{'name': {{'$regex': 'Action', '$options': 'i'}}}}}}\n"
            f"- Query: 'Tell me about a different database.'\n"
            f"  Output: {{'collection': None, 'query': None}}\n\n"
            f"Process this user query: '{state['question']}' and just return only a one-line text JSON result."
        )

        template_search = (
            f"You are an assistant that processes user requests into action search with keyword.\n"
            f"Based on the user's query: '{state['question']}' turn it into a search keyword.\n"
            f"Example output: 'The Dark Knight'\n"
            "Just type the search keyword, or type 'None' if the process is not applicable.\n"
        )

        route = state["route"]

        if route == RouteEnum.NONE or route == RouteEnum.HOME_PAGE or route == RouteEnum.PROFILE_PAGE:
            return {"params": None, "metadata": None, "is_success": True}

        llm = ModelService.get_llm_model(llm_api_key=llm_api_key)
        if route == RouteEnum.CAST_PAGE or route == RouteEnum.MOVIE_PAGE:
            response = await llm.ainvoke(template_movie)
            content = await extract_query_params(response.content)
            collection: str | None = content.get("collection")
            query: dict | None = content.get("query")
            print(f"Collection: {collection}, Query: {query}")
            if not collection or not query:
                return {"query": None, "is_success": False}
            movie_ids = await query_from_database(collection, query)
            if not movie_ids:
                return {"params": None, "metadata": query, "is_success": False}
            else:
                return {"params": {"movie_ids": movie_ids}, "metadata": query, "is_success": True}
        elif route == RouteEnum.GENRE_PAGE:
            response = await llm.ainvoke(template_genre)
            content = await extract_query_params(response.content)
            collection: str | None = content.get("collection")
            query: dict | None = content.get("query")
            print(f"Collection: {collection}, Query: {query}")
            if collection is None or query is None:
                return {"query": None, "is_success": False}
            genre_ids = await query_from_database(collection, query)
            if not genre_ids:
                return {"params": None, "metadata": query, "is_success": False}
            else:
                return {"params": {"genre_ids": genre_ids}, "metadata": query, "is_success": True}
        elif route == RouteEnum.SEARCH_PAGE:
            response = await llm.ainvoke(template_search)
            keyword = response.content.strip()
            print(keyword)
            if keyword.lower() == "none":
                return {"params": None, "metadata": None, "is_success": True}
            else:
                return {"params": {"keyword": keyword}, "metadata": keyword, "is_success": True}

    # Utility function to extract collection name and query from the LLM response
    async def extract_query_params(content: str):
        try:
            content = content.strip()
            if content.startswith("```json"):
                content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            content = (
                content
                .replace("'", "\"")
                .replace("True", "true")
                .replace("False", "false")
                .replace("None","null")
            )
            # Parse JSON
            content = json.loads(content)
            print(f"Parsed content: {content}")
            # Extract collection and query
            collection: str = content.get("collection")
            query: dict = content.get("query")
            return {"collection": collection, "query": query}
        except Exception as e:
            logger.error(f"Error parsing content: {e}")
            # Handle parsing errors
            return {"collection": None, "query": None}


    # Setup MongoDB client (modify with your connection details)
    client = MongoClient(os.getenv("MONGODB_URI"))
    db = client[os.getenv("MONGODB_DB")]

    # Utility function to query the database
    async def query_from_database(collection: str, query: dict) -> list[str] | None:
        if collection is None or query is None:
            return None
        if collection == "movies":
            return await query_movies(query)
        elif collection == "movie_genres":
            return await query_genres(query)
        else:
            return None

    async def query_movies(query: dict) -> list[str] | None:
        print(f"[query_movies] {query}")
        try:
            collection = db["movies"]
            documents = collection.find(query, {"_id": 1}).limit(MAX_ROWS)
            return [str(doc["_id"]) for doc in documents]
        except Exception as e:
            print(f"Error querying 'movies': {e}")
            return None

    async def query_genres(query: dict) -> list[str] | None:
        print(f"[query_genres] {query}")
        try:
            collection = db["movie_genres"]
            documents = collection.find(query, {"_id": 1}).limit(MAX_ROWS)
            return [str(doc["_id"]) for doc in documents]
        except Exception as e:
            print(f"Error querying 'movie_genres': {e}")
            return None

    # Define a new graph
    workflow  = StateGraph(State)
    workflow.add_node("select_route", select_route)
    workflow.add_node("generate_and_query", generate_and_query)
    workflow.add_edge(START, "select_route")
    workflow.add_edge("select_route", "generate_and_query")
    workflow.add_edge("generate_and_query", END)
    return workflow.compile()

async def ai_navigate(llm_api_key: str, question: str):
    print("-- Building graph --")
    graph = build_graph(llm_api_key)
    print("-- Running graph --")
    response = await graph.ainvoke({"question": question})
    print("-- Response from graph --")
    return {
        "route": response.get("route"),
        "params": response.get("params"),
        "metadata": response.get("metadata"),
        "is_success": response.get("is_success")
    }