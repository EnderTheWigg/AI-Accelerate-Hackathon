# Data.gov Fivetran SDK Connector Example (fixed output/warehouse export)
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

import requests
import json
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# global lock for writing the datasets.jsonl safely from threads
_file_lock = threading.Lock()
_OUTPUT_DIR = "output"
_DATASETS_JSONL = os.path.join(_OUTPUT_DIR, "datasets.jsonl")
_WAREHOUSE_DEFAULT = "files/warehouse.db/tester"  # best-effort default used by tester

# ---------------------------------------------------------------------
# Helper: fetch a single page of datasets
# ---------------------------------------------------------------------
def fetch_datasets_page(start: int, page_size: int):
    BASE_URL = "https://catalog.data.gov/api/3/action/package_search"
    params = {"start": start, "rows": page_size}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("result", {}).get("results", [])


# ---------------------------------------------------------------------
# Helper: parse ISO8601 timestamps (handles trailing Z)
# ---------------------------------------------------------------------
def parse_iso8601(iso_str):
    if not iso_str:
        return None
    try:
        if iso_str.endswith("Z"):
            iso_str = iso_str[:-1]
        return datetime.fromisoformat(iso_str)
    except Exception:
        log.warning(f"Invalid ISO8601 format encountered: {iso_str}")
        return None


# ---------------------------------------------------------------------
# Helper: safe append to JSONL from any thread
# ---------------------------------------------------------------------
def append_jsonl_file(path: str, obj: dict):
    # Ensure directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _file_lock:
        with open(path, "a", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, default=str)
            fh.write("\n")


# ---------------------------------------------------------------------
# Helper: process a dataset record and upsert it
# ---------------------------------------------------------------------
def process_dataset_record(ds):
    try:
        formatted = {
            "id": ds.get("id"),
            "name": ds.get("name"),
            "title": ds.get("title"),
            "metadata_created": ds.get("metadata_created"),
            "metadata_modified": ds.get("metadata_modified"),
            "organization": ds.get("organization", {}).get("title") if ds.get("organization") else None,
            "num_resources": len(ds.get("resources", [])),  # INT
            "tags": [tag.get("name") for tag in ds.get("tags", [])],  # JSON array
        }

        # Upsert into destination table
        op.upsert(table="datasets", data=formatted)

        # Append to local JSONL file for debugging (thread-safe)
        append_jsonl_file(_DATASETS_JSONL, formatted)

    except Exception as e:
        # protect against missing id
        ds_id = ds.get("id") if isinstance(ds, dict) else None
        log.severe(f"Error processing dataset {ds_id}", e)


# ---------------------------------------------------------------------
# Define schema function
# ---------------------------------------------------------------------
def schema(configuration: dict):
    return [
        {
            "table": "datasets",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "title": "STRING",
                "metadata_created": "STRING",
                "metadata_modified": "STRING",
                "organization": "STRING",
                "num_resources": "INT",
                "tags": "JSON",
            },
        }
    ]


# ---------------------------------------------------------------------
# Helper: find sqlite file under path or directory
# ---------------------------------------------------------------------
def find_sqlite_file(path: str):
    # direct file
    if os.path.isfile(path):
        return path
    # if the path points to an existing directory, try to find a file inside
    if os.path.isdir(path):
        for fname in os.listdir(path):
            candidate = os.path.join(path, fname)
            if os.path.isfile(candidate):
                # prefer .db/.sqlite extensions, but accept any file as fallback
                if fname.endswith(".db") or fname.endswith(".sqlite"):
                    return candidate
        # fallback: return first file found
        for fname in os.listdir(path):
            candidate = os.path.join(path, fname)
            if os.path.isfile(candidate):
                return candidate
    # try common alternative
    alt = path + ".db"
    if os.path.isfile(alt):
        return alt
    return None


# ---------------------------------------------------------------------
# Helper: export SQLite warehouse.db to JSON Lines (always creates output folder)
# ---------------------------------------------------------------------
def export_warehouse_to_jsonl(db_path=_WAREHOUSE_DEFAULT, out_path=os.path.join(_OUTPUT_DIR, "warehouse_export.jsonl")):
    # Ensure output folder exists no matter what
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    db_file = find_sqlite_file(db_path)
    if not db_file:
        # Create a small JSONL status file so you always have an artifact
        status = {
            "_export_status": "warehouse_db_not_found",
            "checked_paths": [db_path, db_path + ".db"],
            "timestamp": datetime.now().isoformat(),
        }
        append_jsonl_file(out_path, status)
        log.warning(f"Warehouse DB not found (checked: {db_path}). Wrote status to {out_path}")
        return

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get user tables (skip sqlite internal tables)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in cursor.fetchall()]

        with open(out_path, "w", encoding="utf-8") as out_file:
            for table_name in tables:
                cursor.execute(f"SELECT * FROM '{table_name}';")
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description] if cursor.description else []
                for row in rows:
                    record = dict(zip(columns, row))
                    record["_table"] = table_name
                    json.dump(record, out_file, ensure_ascii=False, default=str)
                    out_file.write("\n")

        conn.close()
        log.info(f"Warehouse exported successfully to {out_path} (db_file: {db_file})")
    except Exception as e:
        log.severe("Failed to export warehouse database", e)
        # still write a failure status record
        append_jsonl_file(out_path, {"_export_status": "failed", "error": str(e), "timestamp": datetime.now().isoformat()})


# ---------------------------------------------------------------------
# Define the update function
# ---------------------------------------------------------------------
def update(configuration: dict, state: dict):
    log.info("Starting Data.gov connector update")

    # ensure output dir exists up front so files are created predictably
    os.makedirs(_OUTPUT_DIR, exist_ok=True)

    PAGE_SIZE = int(configuration.get("page_size", "100"))
    MAX_ROWS = int(configuration.get("max_rows", "1000"))
    PARALLELISM = int(configuration.get("parallelism", "4"))

    # Reset JSONL file for new run (safe even if it does not exist)
    try:
        if os.path.exists(_DATASETS_JSONL):
            os.remove(_DATASETS_JSONL)
    except Exception as e:
        log.warning(f"Could not remove {_DATASETS_JSONL} before run: {e}")

    start = state.get("last_start", 0)
    total_fetched = 0

    last_run_iso = state.get("last_run")
    last_run_dt = parse_iso8601(last_run_iso)

    while True:
        try:
            results = fetch_datasets_page(start, PAGE_SIZE)
        except Exception as e:
            log.severe(f"Failed to fetch datasets page starting at {start}", e)
            break

        if not results:
            log.info("No more datasets to fetch")
            break

        with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
            futures = []
            for ds in results:
                mod_dt = parse_iso8601(ds.get("metadata_modified"))
                if last_run_dt and mod_dt and mod_dt <= last_run_dt:
                    continue
                futures.append(executor.submit(process_dataset_record, ds))

            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    log.severe("Error in processing dataset future", e)

        total_fetched += len(results)
        start += PAGE_SIZE

        if total_fetched >= MAX_ROWS:
            log.info(f"Reached max_rows limit: {MAX_ROWS}")
            break

    # Save connector state
    state["last_start"] = start
    state["last_run"] = datetime.now().isoformat()
    op.checkpoint(state)

    # Export the warehouse database to a readable JSONL file (always produces an artifact)
    export_warehouse_to_jsonl()

    log.info("Update completed successfully")


# ---------------------------------------------------------------------
# Connector entry point
# ---------------------------------------------------------------------
connector = Connector(update=update, schema=schema)

# ---------------------------------------------------------------------
# Optional: Run locally for debugging
# ---------------------------------------------------------------------
if __name__ == "__main__":
    with open("configuration.json") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)