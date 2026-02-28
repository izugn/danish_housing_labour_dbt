"""
Entrypoint: fetch all DST tables and load them into Snowflake raw tables.

Usage:
    python run_ingestion.py                # incremental (append mode)
    python run_ingestion.py --full-refresh # overwrite existing tables
"""
import argparse

from dotenv import load_dotenv

from src.fetch import fetch_all_tables
from src.load import load_dataframe
from src.tables import TABLES

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="DST → Snowflake ingestion pipeline")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Overwrite existing Snowflake tables (default: append/incremental)",
    )
    args = parser.parse_args()
    overwrite = args.full_refresh

    print("Starting ingestion pipeline...")
    if overwrite:
        print("Mode: full-refresh (overwrite)")
    else:
        print("Mode: incremental (append)")

    dataframes = fetch_all_tables(TABLES)

    for config in TABLES:
        df = dataframes.get(config.table_id)
        if df is not None and not df.empty:
            load_dataframe(df, table_name=config.snowflake_table, overwrite=overwrite)
        else:
            print(f"WARNING: No data returned for {config.table_id}")

    print("Ingestion complete.")


if __name__ == "__main__":
    main()
