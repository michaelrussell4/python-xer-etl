"""A basic ETL script to extract data from an XER file and load it into a SQLite database."""

# pylint: disable=broad-exception-caught,redefined-outer-name

import sqlite3
import petl as etl
from xerparser import Xer

FILE = "assets/project.xer"
conn = sqlite3.connect("assets/demo.db")


def create_table_if_not_exists(conn, table_name, columns):
    """Create a table in the SQLite database if it does not exist."""
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    )
    if not cursor.fetchone():
        columns_def = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE {table_name} ({columns_def});")
    cursor.close()


try:
    # Read and parse the XER file
    with open(FILE, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # Iterate over XER tables and save them into SQLite
    for name, data in xer.tables.items():
        if isinstance(data, list) and isinstance(data[0], dict):
            table = etl.fromdicts(data)
            columns = data[0].keys()
            create_table_if_not_exists(conn, name, columns)
            etl.todb(table, conn, name)  # Write to SQLite database
        else:
            continue

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()  # Close the connection when finished
