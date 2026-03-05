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
    # If the full request would exceed the DST 1 M-cell limit, set chunk_variable
    # to a variable code (e.g. "Tid") and chunk_size to values per sub-request.
    chunk_variable: str | None = field(default=None)
    chunk_size: int = field(default=10)


TABLES: list[TableConfig] = [
    # --- Housing price sources ---
    TableConfig(
        table_id="EJ56",
        snowflake_table="RAW_PRICE_INDEX",
        description="Property price index (2022=100) by region, category and metric",
        variables=[
            {"code": "OMRÅDE",       "values": ["*"]},  # all regions + All Denmark + provinces
            {"code": "EJENDOMSKATE", "values": ["*"]},  # One-family houses, Owner-occupied flats, Weekend cottages
            {"code": "TAL",          "values": ["*"]},  # Index, QoQ % change, YoY % change
            {"code": "Tid",          "values": ["*"]},  # quarterly from 1992Q1
            # Columns returned by API: OMRÅDE, EJENDOMSKATE, TAL, TID, INDHOLD
            # Note: `..` (suppressed) values appear for some province/property combinations
        ],
    ),
    TableConfig(
        table_id="EJ131",
        snowflake_table="RAW_PRICE_REGIONAL",
        description="Sales key figures by region and property category, monthly",
        variables=[
            {"code": "REGION",       "values": ["*"]},  # all regions + All Denmark
            {"code": "EJENDOMSKATE", "values": ["*"]},  # One-family houses, Owner-occupied flats
            {"code": "BNØGLE",       "values": ["*"]},  # sales count, avg price, purchase sum %, estimated sales
            {"code": "Tid",          "values": ["*"]},  # monthly from 2006M01
            # Columns returned by API: REGION, EJENDOMSKATE, BNØGLE, TID, INDHOLD
            # Note: `..` appears for Owner-occupied flats outside Region Hovedstaden
        ],
    ),
    # --- Labour market / income sources ---
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
        # Full cross-product exceeds DST 1 M-cell CSV limit; chunk by year.
        # 10 years × ~110 regions × 4 units × ~39 income types ≈ 172 k cells/chunk.
        chunk_variable="Tid",
        chunk_size=10,
    ),
    # --- National housing benchmark ---
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
