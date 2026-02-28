"""
Load DataFrames into Snowflake raw tables using snowflake-connector-python.
"""
import os

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def _get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names for Snowflake compatibility.

    Strips BOM (\\ufeff), leading/trailing whitespace, uppercases, and
    replaces spaces with underscores. Protects against corrupt column
    names from DST CSV headers.
    """
    df = df.copy()
    df.columns = [
        c.lstrip("\ufeff").strip().upper().replace(" ", "_")
        for c in df.columns
    ]
    return df


def table_exists(conn: snowflake.connector.SnowflakeConnection, table_name: str) -> bool:
    """Return True if table_name already exists in the current schema."""
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
        return len(cur.fetchall()) > 0
    finally:
        cur.close()


def load_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> None:
    """Write a DataFrame to a Snowflake table, creating it if it doesn't exist.

    Args:
        df:         Data to load.
        table_name: Target Snowflake table (case-insensitive; uppercased internally).
        overwrite:  If True, truncate and replace the table (full-refresh).
                    If False (default), append rows (incremental mode).
    """
    df = _normalize_columns(df)

    conn = _get_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name=table_name.upper(),
            auto_create_table=True,
            overwrite=overwrite,
        )
        if not success:
            raise RuntimeError(
                f"write_pandas reported failure for table '{table_name}': "
                f"{nrows} rows attempted across {nchunks} chunk(s)"
            )
        mode = "overwrite" if overwrite else "append"
        print(
            f"  → {table_name}: {nrows:,} rows loaded "
            f"({nchunks} chunk(s), mode={mode})"
        )
    finally:
        conn.close()
