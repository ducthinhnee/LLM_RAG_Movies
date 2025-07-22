# RAG-LLM v0.3
## Introduction
This repository contains the python code to store knowledge base and retrieve related movies by query.
This is v0.3 of the project. The project is built on top of FastAPI.
### Main features
- **Knowledge Base API:** Manage the knowledge base (sync, drop, collections).
- **Retriever API:** Retrieve related movies by query in given collection.
- **RAG API:** Using LLM adn related docs retrieved from knowledge base to generate response.
- **Navigation API:** Return type of route and params to navigate the web by query.
### New updates
- You can select Gemini model or OpenAI model by changing ```USE_GEMINI=TRUE``` in .env file.
- New features: 
  - Navigate the web by query. (Consuming a lot of tokens to analysis query and generate best answer for you).
  - RAG API. (Consuming tokens than navigate API).
## Container Setup
1. Clone the repository.
2. Create .env file from .env.example (at the same level of .env.example).
3. Build the docker image using docker-compose.
    ```bash
    docker-compose up
    ```
4. Let create the token to access knowledge base by running create token API.
    ```bash
    curl -X 'POST' \
      'http://localhost:8000/create-token/' \
      -H 'accept: application/json' \
      -d ''
    ```
    The token will be printed out in console and log file (at your local machine).
    The token is used to manage the knowledge base.
5. Run sync knowledge base API to embed all data of mongodb into knowledge base. 
If you have your own mongodb, please update the MONGODB_URI and MONGODB_DB in .env file.
It will consume a lot of time to embed all data into knowledge base.

**Note:** If you want to watch the logs easily in app container, you can modify configuration of structlog.
```
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)
```
## Usage
1. Run the docker container.
2. Open the browser and go to `http://localhost:8000/docs` (swagger UI) and `http://localhost:8000/redoc` (redoc UI).
3. To developer, they should use retriever API to retrieve the related movies by query.
4. To admin, they should use knowledge base API to manage the knowledge base.

## How to get google api key
1. Go to https://ai.google.dev/gemini-api/docs/.
2. Click on `Get a Gemini API Key` button.
3. Create API key and copy it.

## Project structure
```
|-- app
|   |-- api
|   |   |-- __init__.py
|   |   |-- create_token.py
|   |   |-- knowledge_base.py
|   |   |-- navigation.py
|   |   |-- rag.py
|   |   |-- retriever.py
|   |-- models
|   |   |-- request_models.py
|   |   |-- response_models.py
|   |-- services
|   |   |-- knowledge_base_service.py
|   |   |-- model_service.py
|   |   |-- navigation_service.py
|   |   |-- rag_service.py
|   |   |-- retriever_service.py
|   |   |-- tmdb_service.py
|   |-- utils
|   |   |-- decode_jwt.py
|   |   |-- exceptions.py
|   |   |-- transform_document.py
|   |   |-- vector_store.py
|   |-- main.py
|-- run.py
|-- ...
```

- **api:** Contains the API endpoints.
- **models:** Contains the request and response models (interface).
- **services:** Contains the business logic.
- **utils:** Contains the utility functions.
- **main.py:** Contains the FastAPI application.
- **run.py:** Contains the code to run the FastAPI application.

## API Endpoints
### APIs used for building web applications (developer)
- **GET /healthy:** Check the health of the service.
- **GET /knowledge_base/collections:** Get all collections in knowledge base.
- **GET /retriever/:** Retrieve related movies by query in given collection
    - **llm_api_key:** LLM API key (Use gemini model as default to embed the data). 
  If you want to use OpenAI model, please set ```USE_GEMINI=FALSE``` in .env file).
    - **collection:** Collection name in knowledge base (Search in this collection).
    - **query:** Query to search related movies (Retrieve related movies by this query).
    - **amount:** Amount of related movies to retrieve.
    - **threshold:** Threshold to filter the related movies.
- **POST /rag/:** Using LLM adn related docs retrieved from knowledge base to generate response
    - **llm_api_key:** LLM API key (Use gemini model as LLM to analysis query and generate the best answer).
    - **collection:** Collection name in knowledge base (Search in this collection).
    - **query:** Query to ask LLM about the movie that you want to watch.
- **GET /navigate/:** Return type of route and params to navigate the web by query
    - **llm_api_key:** LLM API key (Use gemini model as LLM to analysis query and generate the best answer). 
  If you want to use OpenAI model, please set ```USE_GEMINI=FALSE``` in .env file).
    - **query:** Prompt to navigate the web.
### APIs manage knowledge base (Should be used by admin)
- **POST /create-token/:** Create token to access knowledge base (Print out in console and log file).
- **POST /knowledge-base/sync:** Sync knowledge base with the given collection.
- **POST /knowledge-base/sync-with-auto:** Sync knowledge base with the given collection with auto
  - **gemini_api_key:** Gemini API key (Use gemini model to embed the data).
  - **token:** Token to access knowledge base.
- **POST /knowledge-base/sync-with-auto-retry:** Sync knowledge base with the given collection with auto retry
  - **gemini_api_key:** Gemini API key (Use gemini model to embed the data).
  - **token:** Token to access knowledge base.
  - **retry_count:** Count of retry to sync knowledge base.
  - **max_retries:** Max retries to sync knowledge base.
- **POST /knowledge-base/drop:** Drop the all collections in knowledge base
  - **gemini_api_key:** Gemini API key (Use gemini model to embed the data).
  - **token:** Token to access1 knowledge base.

**Remember:** 
- The system does not use an LLM to understand the meaning of the query. 
It simply uses the search query with a vector database to find vectors that are most similar to the query vector.
- ID in response of retriever API is the _id of object in TMDB database (```_id = ObjectId(67556a930eac33604a69f72c)```). 
You can use this id to get more information about the movie by querying to mongodb. 
In hosted server, rag-llm service uses database of volunteer team. The id maybe different from the _id in your database.

## More about knowledge store and retrieve API
### Understanding the knowledge store and retrieve API
The knowledge base is created by embedding the entire dataset in MongoDB using PGVector 
(Postgres vector database). When a user sends a query to the server, the server embeds the 
query into a vector and searches for vectors similar to it in the vector database. 
Essentially, the retriever simply performs a search in the vector database and returns 
the vectors most similar to the query vector.

### What is embedding into vector database?
When storing the knowledge base, the RAG-LLM service will access MongoDB to retrieve each object. These objects will have their field names and values extracted and concatenated into a paragraph in the format:
```<name of field 1>: <value of field 1> \n <name of field 2>: <value of field 2> \n ...```

### Suggestion for similar movies
You can gather descriptions of movies or any information you consider important for recommending similar movies and include it in the query. In that case, similar movies will be returned (at least, that’s what I think).

## More about navigation API
### Successful response
```
{
  "status": int,
  "data": {
    "route": PageEnum,
    "params": Object,
    "metadata": Object
  }
}
```
- "status": is the same HTTP Response code:
  - **200**: OK
  - **500**: Internal Server Error
- "route": It is the type of URL the user should be redirected to. 
Has the type PageEnum that is an one of the following strings
```HOME_PAGE, PROFILE_PAGE, SEARCH_PAGE, CAST_PAGE, MOVIE_PAGE, GENRE_PAGE, NONE```
- "params":
  - For ```route``` = ```HOME_PAGE```, ```PROFILE_PAGE```, or ```NONE``` (when no route can be mapped), ```params``` will be ```null```.
  - For ```route``` = ```SEARCH_PAGE```, ```params``` will be an object of type ```{ keyword: string }```.
  - For ```route``` = ```CAST_PAGE``` or ```MOVIE_PAGE```, ```params``` will be an object of type ```{ movie_ids: list[string] }``` or ```null``` if no related movies exist.
  - For ```route``` = ```GENRE_PAGE```, ```params``` will be an object of type ```{ genres_id: list[string] }``` or ```null``` if no related genres exist.
- "metadata": either null or an object containing the executed query.
- "is_success": Boolean, indicating whether the database query was successful. If the query does not access the database, it always will return true.

**Example 1:**
- Requested Query: Casts of Moana
- Response:
```
{
  "status": 200,
  "data": {
    "route": "CAST_PAGE",
    "params": {
      "movie_ids": [
        "67556a930eac33604a69f734",
        "67556a930eac33604a69f744",
        "67556a960eac33604a69f7ba",
        "67556a970eac33604a69f80b",
        "67556b4b0eac33604a6a1afd"
      ]
    },
    "metadata": {
      "title": {
        "$regex": "Moana",
        "$options": "i"
      }
    },
    "is_success": true
  }
}
```

**Example 2:**
- Requested Query: Search with keyword "Tuan Dat"
- Response:
```
{
  "status": 200,
  "data": {
    "route": "SEARCH_PAGE",
    "params": {
      "keyword": "Tuan Dat"
    },
    "metadata": "Tuan Dat",
    "is_success": true
  }
}
```

**Example 3:**

- Requested Query: How does my profile look like?
- Response:
```
{
  "status": 200,
  "data": {
    "route": "PROFILE_PAGE",
    "params": null,
    "metadata": null
  },
  "is_success": true
}
```

## Example about RAG API
- Requested Query: I want to watch drama film, Please suggest for me
- Response:
```
{
  "status": 200,
  "data": {
    "result": "\"Choices,\" an Indian romance drama, explores the consequences of a woman's affair.  \"Sense\" is a drama about a student who reevaluates his life.  \"I Want To Talk\" follows an American-settled man reconnecting with his daughter.\n"
  }
}
```

## Hosted server
- **URL:** https://awd-llm.azurewebsites.net/docs

**Note:** This hosted server will remain active on Azure for 3 to 6 months, after which it will be deactivated.

## MongoDB server (contains collections that are crawled from TMDB)
- **URI:** mongodb+srv://readonly_user:webnc21_3@tmdb.yhr8d.mongodb.net/



