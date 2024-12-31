"""A basic ETL script to extract data from an XER file and load it into a SQLite database."""

# pylint: disable=broad-exception-caught,redefined-outer-name

import sqlite3
import logging
import petl as etl
from xerparser import Xer

DB_PATH = "assets/demo.db"
FILE = "assets/project.xer"


def get_db_connection(db_path):
    return sqlite3.connect(db_path)


def create_table_if_not_exists(conn, table_name, columns):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    )
    if not cursor.fetchone():
        columns_def = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE {table_name} ({columns_def});")
    cursor.close()


def process_xer_file(file, conn):
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    for name, data in xer.tables.items():
        if isinstance(data, list) and isinstance(data[0], dict):
            table = etl.fromdicts(data)
            columns = data[0].keys()
            create_table_if_not_exists(conn, name, columns)
            etl.todb(table, conn, name)  # Write to SQLite database

            # Example transformation: Add a new column with a constant value
            transformed_table = etl.addfield(table, "new_column", "constant_value")

            # Example transformation: Convert all text to uppercase in a specific column
            transformed_table = etl.convert(
                transformed_table, "some_column", lambda v: v.upper()
            )

            # Create a new table for the transformed data
            new_table_name = f"{name}_transformed"
            create_table_if_not_exists(
                conn, new_table_name, transformed_table.fieldnames()
            )
            etl.todb(transformed_table, conn, new_table_name)
        else:
            logging.warning("Skipping invalid table data for %s", name)


def main():
    conn = None
    try:
        conn = get_db_connection(DB_PATH)
        process_xer_file(FILE, conn)
        conn.commit()
    except sqlite3.DatabaseError as db_err:
        logging.error("Database error: %s", db_err)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
