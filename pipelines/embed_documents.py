"""
embed_documents.py

Plain Python (psycopg2-only) ingestion script that embeds all the
capstone's unstructured text sources into one shared table,
document_embeddings, tagged by source_type:

  - 'company_profile'   <- companies.description
  - 'news_article'      <- news_articles.description
  - 'filing_excerpt'     <- company_documents.document_text (document_type='filing_excerpt')
  - 'earnings_summary'   <- company_documents.document_text (document_type='earnings_summary')

Same pattern as embed_weather_documents.py from the Day 2 homework:
chunk -> embed with sentence-transformers/all-MiniLM-L6-v2 (384-dim) ->
write via psycopg2.extras.execute_values with a direct %s::vector cast.

Run:
    python embed_documents.py
"""

from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

import lakebase

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100


def get_unembedded_documents() -> list[dict]:
    """
    Read rows from companies, news_articles, and company_documents that
    don't yet have a corresponding row in document_embeddings, tagged
    with a uniform (source_type, source_id, text) shape.
    """
    return lakebase.run_query(
        """
        SELECT 'company_profile' AS source_type, symbol AS source_id, description AS text
        FROM companies c
        WHERE description IS NOT NULL AND description != ''
          AND NOT EXISTS (
              SELECT 1 FROM document_embeddings de
              WHERE de.source_type = 'company_profile' AND de.source_id = c.symbol
          )

        UNION ALL

        SELECT 'news_article' AS source_type, id AS source_id, description AS text
        FROM news_articles n
        WHERE description IS NOT NULL AND description != ''
          AND NOT EXISTS (
              SELECT 1 FROM document_embeddings de
              WHERE de.source_type = 'news_article' AND de.source_id = n.id
          )

        UNION ALL

        SELECT document_type AS source_type, id AS source_id, document_text AS text
        FROM company_documents d
        WHERE document_text IS NOT NULL AND document_text != ''
          AND NOT EXISTS (
              SELECT 1 FROM document_embeddings de
              WHERE de.source_type = d.document_type AND de.source_id = d.id
          )
        """
    )


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Split text into overlapping chunks. Text shorter than chunk_size
    naturally produces exactly one chunk (the full text) - same logic as
    the Day 2 weather pipeline.
    """
    chunks = []
    step = chunk_size - overlap

    for start in range(0, len(text), step):
        chunk = text[start : start + chunk_size].strip()
        if not chunk:
            continue
        chunks.append(chunk)
        if start + chunk_size >= len(text):
            break

    return chunks


def embed_chunks(model: SentenceTransformer, chunks: list[str]) -> list[list[float]]:
    """Run the model over a list of text chunks, return one vector per chunk."""
    vectors = model.encode(chunks, show_progress_bar=False)
    return vectors.tolist()


def build_embedding_rows(document: dict, chunks: list[str], vectors: list[list[float]]) -> list[dict]:
    """
    Zip a document's chunks + vectors into rows ready for insertion.
    id scheme: f"{source_type}:{source_id}:{chunk_index}" - namespaced by
    source_type so a company_profile and a news_article can never collide
    even if their source_ids happened to be identical strings.
    """
    source_type = document["source_type"]
    source_id = document["source_id"]
    rows = []

    for chunk_index, (chunk_text_value, vector) in enumerate(zip(chunks, vectors)):
        rows.append({
            "id": f"{source_type}:{source_id}:{chunk_index}",
            "source_type": source_type,
            "source_id": source_id,
            "chunk_index": chunk_index,
            "chunk_text": chunk_text_value,
            "embedding": vector,
            "model_name": EMBEDDING_MODEL_NAME,
        })

    return rows


def write_embeddings(rows: list[dict]) -> int:
    """
    Batch-insert rows into document_embeddings using execute_values,
    casting the embedding to ::vector directly in the SQL.
    """
    if not rows:
        return 0

    insert_sql = """
        INSERT INTO document_embeddings (
            id, source_type, source_id, chunk_index, chunk_text, embedding, model_name, created_at
        )
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    template = "(%s, %s, %s, %s, %s, %s::vector, %s, now())"

    data = [
        (
            row["id"],
            row["source_type"],
            row["source_id"],
            row["chunk_index"],
            row["chunk_text"],
            "[" + ",".join(str(float(x)) for x in row["embedding"]) + "]",
            row["model_name"],
        )
        for row in rows
    ]

    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, data, template=template, page_size=100)
            conn.commit()

    return len(rows)


def main():
    print(f"Loading embedding model {EMBEDDING_MODEL_NAME}...")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    documents = get_unembedded_documents()
    print(f"Found {len(documents)} unembedded documents across all source types.")

    by_type = {}
    for doc in documents:
        by_type[doc["source_type"]] = by_type.get(doc["source_type"], 0) + 1
    print(f"Breakdown by source_type: {by_type}")

    all_rows = []
    for doc in documents:
        chunks = chunk_text(doc["text"])
        if not chunks:
            continue
        vectors = embed_chunks(model, chunks)
        rows = build_embedding_rows(doc, chunks, vectors)
        all_rows.extend(rows)

    print(f"Built {len(all_rows)} embedding rows across {len(documents)} documents.")

    written = write_embeddings(all_rows)
    print(f"Wrote {written} rows into document_embeddings.")


if __name__ == "__main__":
    main()
