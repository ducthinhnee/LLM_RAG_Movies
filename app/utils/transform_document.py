from bson import ObjectId
from langchain_core.documents import Document


def serialize_value(value):
    """
    Handles serialization of MongoDB fields to make them JSON serializable.

    Args:
        value: Any value from a MongoDB document.

    Returns:
        Serialized value (JSON-safe).
    """
    if isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, list):
        return [serialize_value(item) for item in value]
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    else:
        return value


def _extract_name_list(items) -> str:
    """Extract 'name' field from a list of dicts, e.g. genres / keywords."""
    if not isinstance(items, list):
        return str(items)
    names = [item.get("name", "") if isinstance(item, dict) else str(item) for item in items]
    return ", ".join(filter(None, names))


def _build_movie_content(doc: dict) -> str:
    """Build a readable embedding text for a movie document."""
    parts = []

    if doc.get("title"):
        parts.append(f"Title: {doc['title']}")
    if doc.get("tagline"):
        parts.append(f"Tagline: {doc['tagline']}")
    if doc.get("overview"):
        parts.append(f"Overview: {doc['overview']}")
    if doc.get("genres"):
        parts.append(f"Genres: {_extract_name_list(doc['genres'])}")
    if doc.get("keywords"):
        parts.append(f"Keywords: {_extract_name_list(doc['keywords'])}")
    if doc.get("release_date"):
        parts.append(f"Release Date: {doc['release_date']}")
    if doc.get("status"):
        parts.append(f"Status: {doc['status']}")
    if doc.get("runtime"):
        parts.append(f"Runtime: {doc['runtime']} minutes")
    if doc.get("vote_average") is not None:
        parts.append(f"Rating: {doc['vote_average']}/10 ({doc.get('vote_count', 0)} votes)")
    if doc.get("spoken_languages"):
        parts.append(f"Languages: {_extract_name_list(doc['spoken_languages'])}")
    if doc.get("production_countries"):
        parts.append(f"Countries: {_extract_name_list(doc['production_countries'])}")

    return "\n".join(parts)


def _build_people_content(doc: dict) -> str:
    """Build a readable embedding text for a person document."""
    parts = []

    if doc.get("name"):
        parts.append(f"Name: {doc['name']}")
    if doc.get("known_for_department"):
        parts.append(f"Known For: {doc['known_for_department']}")
    if doc.get("biography"):
        parts.append(f"Biography: {doc['biography']}")
    if doc.get("birthday"):
        line = f"Born: {doc['birthday']}"
        if doc.get("place_of_birth"):
            line += f" in {doc['place_of_birth']}"
        parts.append(line)
    if doc.get("deathday"):
        parts.append(f"Died: {doc['deathday']}")
    if doc.get("also_known_as"):
        aliases = doc["also_known_as"]
        if isinstance(aliases, list):
            aliases = ", ".join(aliases)
        parts.append(f"Also Known As: {aliases}")
    if doc.get("popularity") is not None:
        parts.append(f"Popularity: {doc['popularity']}")

    return "\n".join(parts)


# Map collection name → page_content builder
_CONTENT_BUILDERS = {
    "movies": _build_movie_content,
    "people": _build_people_content,
}


def transform_document(mongo_doc: dict, collection_name: str = "") -> Document:
    """
    Transforms a MongoDB document into a LangChain Document for vector storage.

    Args:
        mongo_doc (dict): MongoDB document (already projected to needed fields).
        collection_name (str): Name of the source collection, used to select
                               the appropriate page_content builder.

    Returns:
        Document: LangChain Document with page_content and metadata.
    """
    serialized_doc = {field: serialize_value(value) for field, value in mongo_doc.items()}

    # Use collection-specific builder if available, else fall back to generic format
    builder = _CONTENT_BUILDERS.get(collection_name)
    if builder:
        page_content = builder(serialized_doc)
    else:
        page_content = "\n".join(
            [f"{field}: {value}" for field, value in serialized_doc.items()]
        )

    metadata = serialized_doc.copy()
    return Document(page_content=page_content, metadata=metadata)