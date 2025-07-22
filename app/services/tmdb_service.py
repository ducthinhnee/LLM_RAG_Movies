import os
import structlog

from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = structlog.get_logger(__name__)

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

    def fetch_collection_in_batches(self, collection_name: str, batch_size: int = 100):
        """
        Fetch documents from a collection in batches to handle large datasets without no_cursor_timeout.

        Args:
            collection_name (str): The name of the collection.
            batch_size (int): The number of documents to fetch in each batch.

        Yields:
            list: A batch of documents from the collection.
        """
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")

        collection = self.get_collection(collection_name)

        last_id = None  # Start with no last_id
        if self._error_sync:
            last_id = self._current_last_id
            self._clear_error_sync()

        try:
            while True:
                query = {}
                if last_id:  # Fetch only documents greater than the last_id
                    query["_id"] = {"$gt": last_id}

                # Fetch documents with a limit (pagination)
                batch = list(collection.find(query).sort("_id").limit(batch_size))

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
        Stream data from all collections in the database in batches.

        Args:
            batch_size (int): The number of documents to fetch per batch from each collection.

        Yields:
            tuple: The collection name and a batch of documents.
        """
        if self.db is None:
            raise RuntimeError("Database connection is not initialized. Call 'connect()' first.")

        collections = self.list_collections()

        if self._error_sync:
            if self._current_collection in collections:
                start_index = collections.index(self._current_collection)
                collections = collections[start_index:]

        for collection_name in collections:
            print(f"Streaming data from collection: {collection_name}")
            self._current_collection = collection_name
            for batch in self.fetch_collection_in_batches(collection_name, batch_size=batch_size):
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