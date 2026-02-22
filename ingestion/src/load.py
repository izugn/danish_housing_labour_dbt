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


def load_dataframe(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> None:
    """Write a DataFrame to a Snowflake table, creating it if it doesn't exist."""
    conn = _get_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name=table_name.upper(),
            auto_create_table=True,
            overwrite=overwrite,
        )
        print(
            f"  → {table_name}: {nrows} rows loaded "
            f"({nchunks} chunk(s), success={success})"
        )
    finally:
        conn.close()
