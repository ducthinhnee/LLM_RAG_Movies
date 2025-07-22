from bson import ObjectId
from langchain_core.documents import Document


def serialize_value(value):
    """
    Handles serialization of MongoDB fields to make them JSON serializable.

    Args:
        value: Any value from a MongoDB document.

    Returns:
        str: Serialized value.
    """
    if isinstance(value, ObjectId):
        return str(value)  # Convert ObjectId to string
    elif isinstance(value, list):
        return [serialize_value(item) for item in value]  # Recursively serialize lists
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}  # Recursively serialize dicts
    else:
        return value  # Other types remain as-is

def transform_document(mongo_doc):
    """
    Transforms a MongoDB document into the Document format for vector storage.

    Args:
        mongo_doc (dict): MongoDB document.

    Returns:
        Document: Transformed document for vector storage.
    """
    serialized_doc = {field: serialize_value(value) for field, value in mongo_doc.items()}

    page_content = "\n".join([f"{field}: {value}" for field, value in serialized_doc.items()])

    metadata = serialized_doc.copy()
    return Document(page_content=page_content, metadata=metadata)