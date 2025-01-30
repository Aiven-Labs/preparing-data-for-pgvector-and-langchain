import logging
import json
import frontmatter
import pathlib

import arrow
from dotenv import load_dotenv
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from create_tables import conn

load_dotenv()

fmt = r"MMMM[\s+]D[\w+,\s+]YYYY"

# define splitter
splitter = RecursiveCharacterTextSplitter(
    chunk_size=300,
    chunk_overlap=20,
    separators=[".", "!", "?", "\n"],
    keep_separator="end",
)

# define embeddings. These options are all the defaults and not explicitly needed.
embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-mpnet-base-v2",
    model_kwargs={"device": "cpu"},
    encode_kwargs={"normalize_embeddings": False},
)


def load_data(file: pathlib.Path):
    """Chunk data, create embeddings, and index in Postgres."""
    frontmatter_post = frontmatter.loads(
        file.read_text()
    )  # loads the metadata from the file
    base_data = {
        "title": frontmatter_post["title"],
        "show": "Conduit",
        "network": "Relay",
        "network_url": "https://relay.fm",
        "description": frontmatter_post["description"],
        "url": frontmatter_post["url"],
        "pub_date": arrow.get(frontmatter_post["pub_date"], fmt).date().isoformat(),
    }

    try:
        with conn.cursor() as cur:

            transcription_sql = (
                "INSERT INTO transcriptions (title, content, meta) VALUES (%s, %s, %s)"
            )
            cur.execute(
                transcription_sql,
                (
                    frontmatter_post["title"],
                    frontmatter_post.content,
                    json.dumps(base_data),
                ),
            )

            insert_query = """
                    INSERT INTO quotes (content, embedding, transcription_title)
                    VALUES (%s, %s, %s)
            """

            post_chunks = splitter.create_documents([frontmatter_post.content])
            #
            # Batch embedding generation
            chunk_contents = [chunk.page_content for chunk in post_chunks]
            chunk_embeddings = embeddings.embed_documents(chunk_contents)

            # Prepare batch insert data
            docs = [
                (
                    post_chunk.page_content,
                    embedding,
                    frontmatter_post["title"],
                )
                for post_chunk, embedding in zip(post_chunks, chunk_embeddings)
            ]

            print(f"{file.name} - {len(docs)} chunks")
            cur.executemany(insert_query, docs)
            conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error("Error proccessing %s: %s" % (file.name, e))


if __name__ == "__main__":
    transcription_path = pathlib.Path().cwd().parent / "conduit-transcripts/transcripts"
    for file in transcription_path.iterdir():
        load_data(file)
