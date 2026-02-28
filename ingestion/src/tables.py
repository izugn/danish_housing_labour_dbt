"""
Table configurations for DST (Statistics Denmark) API calls.
Each TableConfig defines which DST table to pull and which variables/filters to use.
"""
from dataclasses import dataclass, field


@dataclass
class TableConfig:
    table_id: str         # DST table ID, e.g. "EJEN12"
    snowflake_table: str  # Target Snowflake raw table name
    variables: list[dict] # List of {"code": ..., "values": [...]} dicts
    description: str = field(default="")


TABLES: list[TableConfig] = [
    TableConfig(
        table_id="EJEN12",
        snowflake_table="RAW_HOUSING_PRICES",
        description="Real property sales prices by property type and municipality, annual",
        variables=[
            {"code": "EJENDOMSKATE", "values": ["*"]},  # required by API
            {"code": "OMRÅDE",       "values": ["*"]},  # municipality
            {"code": "Tid",          "values": ["*"]},  # time period
        ],
    ),
    TableConfig(
        table_id="AUL01",
        snowflake_table="RAW_UNEMPLOYMENT",
        description="Unemployment by type and municipality, annual",
        variables=[
            {"code": "YDELSESTYPE", "values": ["*"]},  # all unemployment types
            {"code": "OMRÅDE",      "values": ["*"]},  # all municipalities
            {"code": "Tid",         "values": ["*"]},  # annual periods
        ],
    ),
    TableConfig(
        table_id="INDKP101",
        snowflake_table="RAW_LOCAL_INCOME",
        description="Personal income by municipality, annual",
        variables=[
            {"code": "OMRÅDE",       "values": ["*"]},  # all municipalities
            {"code": "ENHED",        "values": ["*"]},  # amount and average
            {"code": "INDKOMSTTYPE", "values": ["*"]},  # all income types
            {"code": "Tid",          "values": ["*"]},  # annual periods
        ],
    ),
    TableConfig(
        table_id="LONS10",
        snowflake_table="RAW_NATIONAL_EARNINGS",
        description="National earnings by industry and earnings measure, annual",
        variables=[
            {"code": "LØNMÅL", "values": ["*"]},  # all earnings measures
            {"code": "Tid",    "values": ["*"]},  # annual periods
        ],
    ),
    TableConfig(
        table_id="LABY22",
        snowflake_table="RAW_HOUSING_NATIONAL",
        description="Key figures for sales of real property by municipality groups, annual",
        variables=[
            {"code": "EJENDOMSKATE", "values": ["*"]},  # property category
            {"code": "BNØGLE",       "values": ["*"]},  # key figures
            {"code": "Tid",          "values": ["*"]},  # annual periods
        ],
    ),
]
