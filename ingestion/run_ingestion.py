"""
Entrypoint: fetch all DST tables and load them into Snowflake raw tables.

Usage:
    python run_ingestion.py
"""
from dotenv import load_dotenv

from src.fetch import fetch_all_tables
from src.load import load_dataframe
from src.tables import TABLES

load_dotenv()


def main() -> None:
    print("Starting ingestion pipeline...")
    dataframes = fetch_all_tables(TABLES)

    for config in TABLES:
        df = dataframes.get(config.table_id)
        if df is not None and not df.empty:
            load_dataframe(df, table_name=config.snowflake_table, overwrite=True)
        else:
            print(f"WARNING: No data returned for {config.table_id}")

    print("Ingestion complete.")


if __name__ == "__main__":
    main()
