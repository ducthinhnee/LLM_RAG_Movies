import os
import structlog

from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = structlog.get_logger(__name__)

# Chỉ embed 2 collection này, mỗi collection chỉ lấy các field được chỉ định.
# Thêm/bỏ field tại đây để điều chỉnh nội dung embedding.
COLLECTION_CONFIG: dict[str, list[str]] = {
    "movies": [
        "tmdb_id",
        "title",
        "original_title",
        "overview",
        "genres",
        "keywords",
        "release_date"
    ],
    "people": [
        "tmdb_id",
        "name",
        "known_for_department",
        "biography",
        "place_of_birth"
    ],
}

class TMDBService:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TMDBService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):  # Prevent reinitialization
            self._initialized = True
            self._uri = os.getenv('MONGODB_URI')  # Fixed MongoDB URI
            self._database_name =os.getenv('MONGODB_DB')  # Fixed database name
            self.client = None
            self.db = None

            # Handle crash
            self._error_sync = False
            self._current_last_id = None
            self._current_collection = None

    def connect(self):
        try:
            if self.client is None:
                self.client = MongoClient(self._uri)
                self.db = self.client[self._database_name]
                print(f"Connected to database: {self._database_name}")
        except PyMongoError as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise e

    def list_collections(self):
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")
        return self.db.list_collection_names()

    def get_collection(self, collection_name: str):
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")
        return self.db[collection_name]

    def insert_document(self, collection_name: str, document: dict):
        collection = self.get_collection(collection_name)
        result = collection.insert_one(document)
        return result.inserted_id

    def find_documents(self, collection_name: str, query: dict = None):
        collection = self.get_collection(collection_name)
        if query is None:
            query = {}
        return list(collection.find(query))

    def update_document(self, collection_name: str, query: dict, update_data: dict):
        collection = self.get_collection(collection_name)
        result = collection.update_one(query, {"$set": update_data})
        return result.modified_count

    def delete_documents(self, collection_name: str, query: dict):
        collection = self.get_collection(collection_name)
        result = collection.delete_many(query)
        return result.deleted_count

    def fetch_collection_in_batches(
        self,
        collection_name: str,
        batch_size: int = 100,
        fields: list[str] | None = None,
    ):
        """
        Fetch documents from a collection in batches to handle large datasets without no_cursor_timeout.

        Args:
            collection_name (str): The name of the collection.
            batch_size (int): The number of documents to fetch in each batch.
            fields (list[str] | None): Specific fields to project. If None, all fields are returned.

        Yields:
            list: A batch of documents from the collection.
        """
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")

        collection = self.get_collection(collection_name)

        # Build MongoDB projection — always include _id for cursor pagination
        projection = None
        if fields:
            projection = {field: 1 for field in fields}
            projection["_id"] = 1  # always needed for pagination

        last_id = None  # Start with no last_id
        if self._error_sync:
            last_id = self._current_last_id
            self._clear_error_sync()

        try:
            while True:
                query = {}
                if last_id:  # Fetch only documents greater than the last_id
                    query["_id"] = {"$gt": last_id}

                # Fetch documents with a limit and optional projection
                cursor = collection.find(query, projection).sort("_id").limit(batch_size)
                batch = list(cursor)

                if not batch:  # Stop when there are no more documents
                    break

                yield batch  # Yield the current batch

                # Update last_id to the _id of the last document in the batch
                last_id = batch[-1]["_id"]
                self._current_last_id = last_id
        except PyMongoError as e:
            logger.error(f"Error while fetching documents: {e}")
            raise e

    def stream_all_collections_data(self, batch_size: int = 100):
        """
        Stream data from the configured collections (COLLECTION_CONFIG) in batches.
        Only collections listed in COLLECTION_CONFIG are processed, and only their
        specified fields are fetched from MongoDB.

        Args:
            batch_size (int): The number of documents to fetch per batch from each collection.

        Yields:
            tuple: The collection name and a batch of documents.
        """
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")

        available_collections = self.list_collections()

        # Only process collections defined in COLLECTION_CONFIG
        target_collections = [
            name for name in COLLECTION_CONFIG if name in available_collections
        ]

        if not target_collections:
            logger.warning(
                "None of the target collections found in the database",
                target=list(COLLECTION_CONFIG.keys()),
                available=available_collections,
            )

        if self._error_sync and self._current_collection in target_collections:
            start_index = target_collections.index(self._current_collection)
            target_collections = target_collections[start_index:]

        for collection_name in target_collections:
            fields = COLLECTION_CONFIG[collection_name]
            print(f"Streaming '{collection_name}' | fields: {fields}")
            self._current_collection = collection_name
            for batch in self.fetch_collection_in_batches(
                collection_name, batch_size=batch_size, fields=fields
            ):
                yield collection_name, batch

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self._current_last_id = None
            self._current_collection = None
            print("Database connection closed.")

    def raise_error_sync(self):
        self._error_sync = True

    def _clear_error_sync(self):
        self._error_sync = False