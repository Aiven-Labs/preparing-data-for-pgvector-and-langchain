import logging
import os
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()

logging.basicConfig(level=logging.INFO)

# Connection parameters
_CONNECTION_STRING = os.getenv("AIVEN_POSTGRES_SERVICE_URI")
conn = psycopg.connect(_CONNECTION_STRING)


def create_table(cursor, table: str, fields_str: str) -> None:
    cursor.execute(
        f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{table}');"
    )
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        cursor.execute(f"CREATE TABLE {table} ({fields_str});")
        logging.info("%s table created successfully." % table)
    else:
        logging.debug("%s table already exists." % table)


if __name__ == "__main__":

    try:
        # Establish the connection
        with conn.cursor() as cur:
            # Check if vector extension exists
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector');"
            )
            extension_exists = cur.fetchone()[0]

            if not extension_exists:
                cur.execute("CREATE EXTENSION vector;")
                logging.info("Vector Extension Created")
            else:
                logging.debug("Vector extension already exists.")

            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector');"
            )

            transcription_fields = (
                "title TEXT PRIMARY KEY",
                "content TEXT",
                "meta JSONB",
            )
            create_table(cur, "transcriptions", ",".join(transcription_fields))

            quote_chunks_fields = (
                "id SERIAL PRIMARY KEY",
                "content TEXT",
                "embedding vector(768)",
                "transcription_title TEXT REFERENCES transcriptions(title)",  # Foreign key to transcriptions table
            )

            create_table(cur, "quotes", ",".join(quote_chunks_fields))
            conn.commit()

    except Exception as exc:
        print(f"{exc.__class__.__name__}: {exc}")
