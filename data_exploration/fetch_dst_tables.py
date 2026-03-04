"""
Fetch sample data from Statistics Denmark (DST) API.

Retrieves recent rows from each of the three DST tables used by this project
and writes them as semicolon-delimited CSV files, alongside a JSON metadata
file for each table.

Tables fetched:
  - EJEN12    -> ejen12_housing_prices.csv    (property sale prices per m²)
  - AUL01     -> aul01_unemployment.csv       (registered unemployment by area)
  - LONS10    -> lons10_earnings.csv          (average monthly earnings by industry)
  - EJEN77    -> ejen77.csv                  (EJEN77 sample data)
  - EJ56      -> ej56.csv                    (EJ56 sample data)
  - LABY22    -> laby22.csv                  (LABY22 sample data)
  - EJ131     -> ej131.csv                   (EJ131 sample data)
  - INDKP101  -> indkp101.csv               (personal income by municipality)

Dependencies: requests  (already listed in ingestion/requirements.txt)

Usage:
    # Default: fetch last 12 periods per table
    python data_exploration/fetch_dst_tables.py

    # Fetch last 24 periods
    python data_exploration/fetch_dst_tables.py --periods 24

    # Fetch all available data (slow, large)
    python data_exploration/fetch_dst_tables.py --all-periods

    # Explore a single table interactively
    python data_exploration/fetch_dst_tables.py --explore EJEN12
"""

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path

import requests
from requests import HTTPError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(format="%(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DST_API_BASE = "https://api.statbank.dk/v1"
OUTPUT_DIR = Path(__file__).parent / "data_sample"

# Cell count threshold at which we warn the user (hard limit is 1,000,000)
CELL_COUNT_WARNING_THRESHOLD = 500_000


# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

@dataclass
class TableConfig:
    table_id: str
    filename: str
    description: str
    variables: list[dict] = field(default_factory=list)


TABLES: list[TableConfig] = [
    TableConfig("EJEN12", "ejen12_housing_prices.csv", "Property sale prices per m²",
                [{"code": "EJENDOMSKATE", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("AUL01", "aul01_unemployment.csv", "Registered unemployment by area",
                [{"code": "YDELSESTYPE", "values": ["*"]}, {"code": "OMRÅDE", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("LONS10", "lons10_earnings.csv", "Average monthly earnings by industry",
                [{"code": "LØNMÅL", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("EJEN77", "ejen77.csv", "EJEN77 sample data", [{"code": "OMRÅDE", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("EJ56",   "ej56.csv",   "EJ56 sample data",   [{"code": "OMRÅDE", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("LABY22", "laby22.csv", "LABY22 sample data", [{"code": "KOMGRP", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig("EJ131",  "ej131.csv",  "EJ131 sample data",  [{"code": "REGION", "values": ["*"]}, {"code": "Tid", "values": None}]),
    TableConfig(
        "INDKP101", "indkp101.csv", "Personal income by municipality",
        [
            {"code": "OMRÅDE",       "values": ["*"]},           # all 98 municipalities + aggregates
            {"code": "ENHED",        "values": ["110", "116"]},  # income amount (DKK 1000) + avg for all (DKK)
            {"code": "INDKOMSTTYPE", "values": ["100", "115", "290"]},  # disposable, wages & salaries, taxable
            {"code": "Tid",          "values": None},             # resolved via --periods / --all-periods
            # KOEN (sex) is eliminable → skipped → DST returns men+women total
        ],
    ),
]


# ---------------------------------------------------------------------------
# API helpers — all use POST as recommended by the DST API docs
# ---------------------------------------------------------------------------


def _dst_post(endpoint: str, payload: dict, *, as_text: bool = False) -> dict | list | str:
    """POST to a DST API endpoint; return parsed JSON or raw text."""
    url = f"{DST_API_BASE}/{endpoint}"
    response = requests.post(url, json={"lang": "en", **payload}, timeout=120)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        raise HTTPError(
            f"{exc} | URL: {url} | Response: {response.text[:400]}", response=response
        ) from exc
    return response.text if as_text else response.json()


def fetch_table_metadata(table_id: str) -> dict:
    """Fetch full table metadata via POST to /tableinfo."""
    return _dst_post("tableinfo", {"table": table_id, "format": "JSON"})


def fetch_table_csv(table_id: str, variables: list[dict]) -> str:
    """POST to /data and return raw CSV text (semicolon-separated)."""
    return _dst_post("data", {
        "table": table_id,
        "format": "CSV",
        "timeOrder": "Ascending",
        "variables": variables,
    }, as_text=True)


# ---------------------------------------------------------------------------
# Variable resolution
# ---------------------------------------------------------------------------


def pick_last_n_periods(time_values: list[dict], n_periods: int | None) -> list[str]:
    """Return the last n_periods IDs, or ["*"] for all."""
    return ["*"] if n_periods is None else [v["id"] for v in time_values[-n_periods:]]


def resolve_variables(
    metadata: dict,
    configured: list[dict],
    n_periods: int | None,
) -> tuple[list[dict], list[str]]:
    """
    Merge user-configured variable selections with table metadata.

    Returns (resolved_variables, warning_messages).
    """
    configured_by_code = {
        v["code"].upper(): v["values"]
        for v in configured
        if v.get("values") is not None
    }
    resolved: list[dict] = []
    warnings: list[str] = []

    for var in metadata.get("variables", []):
        code = var["id"].upper()

        if var.get("time"):
            resolved.append({"code": var["id"], "values": pick_last_n_periods(var.get("values", []), n_periods)})
        elif code in configured_by_code:
            resolved.append({"code": var["id"], "values": configured_by_code[code]})
        elif var.get("elimination"):
            continue
        else:
            warnings.append(
                f"Variable '{var['id']}' ({var.get('text', '')}) is required "
                f"but not configured — selecting all {len(var.get('values', []))} values."
            )
            resolved.append({"code": var["id"], "values": ["*"]})

    return resolved, warnings


def estimate_cell_count(metadata: dict, variables: list[dict]) -> int:
    """Rough cell-count estimate: rows × (n_variables + 1 for INDHOLD)."""
    var_lookup = {v["id"].upper(): v for v in metadata.get("variables", [])}
    value_counts = []
    for req_var in variables:
        meta_var = var_lookup.get(req_var["code"].upper(), {})
        total = len(meta_var.get("values", []))
        selected = req_var["values"]
        value_counts.append(total if selected == ["*"] else min(len(selected), total))

    if not value_counts:
        return 0
    rows = 1
    for vc in value_counts:
        rows *= max(vc, 1)
    return rows * (len(value_counts) + 1)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def normalise_header(name: str) -> str:
    return name.strip().upper().replace(" ", "_")


def save_metadata(metadata: dict, out_path: Path) -> None:
    """Save a human-readable metadata summary as JSON next to the CSV."""
    summary = {
        "id": metadata.get("id"),
        "title": metadata.get("text"),
        "unit": metadata.get("unit"),
        "last_updated": metadata.get("updated"),
        "variables": [
            {
                "code": v["id"],
                "label": v.get("text"),
                "time": v.get("time", False),
                "eliminable": v.get("elimination", False),
                "n_values": len(v.get("values", [])),
                "sample_values": [
                    {"code": val["id"], "label": val.get("text")}
                    for val in v.get("values", [])[:5]
                ],
            }
            for v in metadata.get("variables", [])
        ],
    }
    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("     Metadata → %s", meta_path.name)


def parse_and_save_csv(raw_text: str, out_path: Path) -> int:
    """Parse raw CSV text, normalise headers, write to file. Returns row count."""
    reader = csv.reader(StringIO(raw_text), delimiter=";")
    rows_written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        for i, row in enumerate(reader):
            if i == 0:
                writer.writerow([normalise_header(col) for col in row])
            else:
                writer.writerow(row)
                rows_written += 1
    return rows_written


# ---------------------------------------------------------------------------
# Explore mode
# ---------------------------------------------------------------------------


def explore_table(table_id: str) -> None:
    """Print a detailed variable/value breakdown for a table."""
    log.info("\n%s", "=" * 60)
    log.info("  Exploring table: %s", table_id.upper())
    log.info("%s\n", "=" * 60)

    meta = fetch_table_metadata(table_id.upper())
    log.info("  Title      : %s", meta.get("text"))
    log.info("  Unit       : %s", meta.get("unit"))
    log.info("  Updated    : %s", meta.get("updated"))
    log.info("  Contact    : %s\n", ", ".join(c.get("mail", "") for c in meta.get("contacts", [])))

    for var in meta.get("variables", []):
        values = var.get("values", [])
        flags = (["TIME"] if var.get("time") else []) + (["ELIMINABLE"] if var.get("elimination") else [])
        flag_str = f"  [{', '.join(flags)}]" if flags else ""

        log.info("  Variable: %s — %s%s", var["id"], var.get("text"), flag_str)
        log.info("    Total values: %d", len(values))
        for v in values[:8]:
            log.info("      %10s  %s", v["id"], v.get("text", ""))
        if len(values) > 8:
            log.info("      ... and %d more", len(values) - 8)
        log.info("")


# ---------------------------------------------------------------------------
# Main fetch loop
# ---------------------------------------------------------------------------


def main(n_periods: int | None = 12, all_periods: bool = False, explore: str | None = None) -> None:

    if explore:
        explore_table(explore)
        return

    effective_periods = None if all_periods else n_periods
    period_label = "all periods" if effective_periods is None else f"last {effective_periods} periods"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("\nDST Data Exploration — fetching %s\n", period_label)

    results_summary = []

    for table in TABLES:
        tid = table.table_id
        out_path = OUTPUT_DIR / table.filename

        log.info("%s", "─" * 50)
        log.info("  Table   : %s — %s", tid, table.description)

        # 1. Fetch metadata
        log.info("  Fetching metadata ...")
        meta = fetch_table_metadata(tid)
        log.info("  ✓  (updated: %s)", meta.get("updated", "unknown"))
        log.info("  Title   : %s", meta.get("text"))
        log.info("  Unit    : %s", meta.get("unit"))

        # 2. Resolve variables
        resolved, warnings = resolve_variables(meta, table.variables, effective_periods)
        for w in warnings:
            log.warning("  ⚠  %s", w)
        log.info("  Variables selected:")
        for v in resolved:
            log.info("    %15s = %s", v["code"], v["values"])

        # 3. Estimate cell count and warn if large
        est_cells = estimate_cell_count(meta, resolved)
        if est_cells > CELL_COUNT_WARNING_THRESHOLD:
            log.warning(
                "  ⚠  Estimated cell count: %s (DST hard limit: 1,000,000). Consider reducing --periods.",
                f"{est_cells:,}",
            )
        else:
            log.info("  Estimated cells: ~%s", f"{est_cells:,}")

        # 4. Fetch data
        log.info("  Fetching data ...")
        try:
            raw_text = fetch_table_csv(tid, resolved)
        except HTTPError as exc:
            log.error("  ERROR: %s", exc)
            results_summary.append({"table": tid, "status": "ERROR", "rows": 0})
            continue

        # 5. Parse and save
        rows = parse_and_save_csv(raw_text, out_path)
        log.info("  ✓  %s rows", f"{rows:,}")
        log.info("     Data     → %s", out_path.name)
        save_metadata(meta, out_path)

        results_summary.append({"table": tid, "status": "OK", "rows": rows})

    # Final summary
    log.info("\n%s", "=" * 50)
    log.info("  SUMMARY")
    log.info("%s", "=" * 50)
    for r in results_summary:
        icon = "✓" if r["status"] == "OK" else "✗"
        log.info("  %s  %-8s  %8s rows   [%s]", icon, r["table"], f"{r['rows']:,}", r["status"])
    log.info("\n  Output directory: %s\n", OUTPUT_DIR)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch sample data from the Statistics Denmark (DST) StatBank API."
    )
    parser.add_argument(
        "--periods", type=int, default=12, metavar="N",
        help="Number of recent time periods to fetch per table (default: 12).",
    )
    parser.add_argument(
        "--all-periods", action="store_true",
        help="Fetch all available periods (ignores --periods). May be large.",
    )
    parser.add_argument(
        "--explore", metavar="TABLE_ID",
        help="Print a detailed variable/value breakdown for a table and exit.",
    )
    args = parser.parse_args()

    try:
        main(n_periods=args.periods, all_periods=args.all_periods, explore=args.explore)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(1)
