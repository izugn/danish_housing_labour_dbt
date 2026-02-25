"""
Table configurations for DST (Statistics Denmark) API calls.
Each TableConfig defines which DST table to pull and which variables/filters to use.
"""
from dataclasses import dataclass


@dataclass
class TableConfig:
    table_id: str         # DST table ID, e.g. "EJEN12"
    snowflake_table: str  # Target Snowflake raw table name
    variables: list[dict] # List of {"code": ..., "values": [...]} dicts


TABLES: list[TableConfig] = [
    TableConfig(
        table_id="EJEN12",
        snowflake_table="RAW_HOUSING_PRICES",
        variables=[
            {"code": "EJENDOMSKATE", "values": ["*"]},  # required by API
            {"code": "Tid",    "values": ["*"]},   # time period
        ],
    ),
    TableConfig(
        table_id="AUL01",
        snowflake_table="RAW_UNEMPLOYMENT",
        variables=[
            {"code": "YDELSESTYPE", "values": ["*"]},  # required by API
            {"code": "OMRÅDE", "values": ["*"]},
            {"code": "Tid",    "values": ["*"]},
        ],
    ),
    TableConfig(
        table_id="LONS10",
        snowflake_table="RAW_EARNINGS",
        variables=[
            {"code": "LØNMÅL",   "values": ["*"]},  # required by API
            {"code": "Tid",     "values": ["*"]},
        ],
    ),
]
