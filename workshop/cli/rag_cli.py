import os
import pathlib
import logging

import typer
from dotenv import load_dotenv
from langchain_ollama.chat_models import ChatOllama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_huggingface import HuggingFaceEmbeddings

from data_prep import load_data
from create_tables import conn
from psycopg.rows import dict_row

app = typer.Typer()

load_dotenv()


@app.command()
def upload(file: pathlib.Path):
    """Chunks and uploads a document to the index"""
    typer.echo(f"Uploading {file.name}")
    load_data(file)
    typer.echo("Done!")


@app.command()
def search(query: str, k_results: int = 3):
    """Perform a RAG search using llama3.3"""

    typer.echo("Preparing Embeddings")
    embeddings = HuggingFaceEmbeddings()
    typer.echo("DONE")

    query_embed = f"[{", ".join(str(x) for x in embeddings.embed_query(query))}]"

    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM quotes ORDER BY embedding <-> %s LIMIT %s;",
                (
                    query_embed,
                    k_results,
                ),
            )
            rows = cur.fetchall()
            episodes = set()

            for row in rows:
                episodes.add(row["transcription_title"])

        print(f'Here are some episodes that might help you with "{query}":')
        print("\n".join(episodes))
        print("-------------------")

        llm = ChatOllama(model="llama3.2")
        prompt = ChatPromptTemplate.from_template(
            """
                Offer supportive advice for the question {query} with supporting quotes from
                ---
                "{docs}".
                ---

                If there are no documents to quote, say "I don't have any information on that."

                Mention the quote you're pulling from
                Don't quotes from other sources.
                Make responses about 600 characters
            """
        )

        chain = prompt | llm | StrOutputParser()
        topic = {
            "query": query,
            "docs": "\n".join([result["content"] for result in rows]),
        }
        for chunks in chain.stream(topic):
            print(chunks, end="", flush=True)

    except Exception as exc:
        print()
        logging.warning("%s: %s" % (exc.__class__.__name__, exc))


if __name__ == "__main__":
    app()
