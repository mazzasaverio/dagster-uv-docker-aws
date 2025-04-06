from pydantic import BaseModel
from typing import List, Dict, Any
import json
from datetime import datetime

import dagster as dg
from dagster import MetadataValue
from resources import duckdb


class DuckDBStorageConfig(dg.Config):
    table_name: str = "documents"
    schema_mapping: Dict[str, str] = {
        "document_id": "VARCHAR",
        "filename": "VARCHAR",
        "company_name": "VARCHAR",
        "report_year": "INTEGER",
        "title": "VARCHAR",
        "authors": "JSON",
        "abstract": "TEXT",
        "keywords": "JSON",
        "methodology": "TEXT",
        "key_findings": "JSON",
        "conclusions": "TEXT",
        "proposed_models": "JSON",
        "future_work": "TEXT",
        "metadata": "JSON",
    }


@dg.asset(
    compute_kind="duckdb",
    group_name="load_to_database",
    deps=["extract_structured_info"],
    code_version="v1",
    required_resource_keys={"duckdb"},
)
def load_to_database(
    context: dg.AssetExecutionContext,
    config: DuckDBStorageConfig,
    extract_structured_info: Dict[str, Dict],
) -> dg.Output[Dict[str, Any]]:
    """Load structured data into DuckDB database."""
    context.log.info("Starting database load")

    try:
        # Get duckdb resource from context
        duckdb_resource = context.resources.duckdb

        # Create table if not exists
        duckdb_resource.create_table(config.table_name, config.schema_mapping)

        # Prepare records for insertion
        records = []
        for doc_id, doc_data in extract_structured_info.items():
            # Extract the extraction_date from doc_data
            extraction_date = (
                doc_data.pop("extraction_date")
                if "extraction_date" in doc_data
                else None
            )

            # Extract the json_data from doc_data
            json_data = doc_data.pop("json_data") if "json_data" in doc_data else {}

            # Create metadata JSON
            metadata = {
                "extraction_date": extraction_date,
                "processing_date": datetime.now().isoformat(),
            }

            # Create the record with flattened json_data fields
            record = {
                "document_id": doc_id,
                "filename": doc_data.get("filename", ""),
                "title": json_data.get("title", ""),
                "authors": json.dumps(json_data.get("authors", [])),
                "abstract": json_data.get("abstract", ""),
                "keywords": json.dumps(json_data.get("keywords", [])),
                "methodology": json_data.get("methodology", ""),
                "key_findings": json.dumps(json_data.get("key_findings", [])),
                "conclusions": json_data.get("conclusions", ""),
                "proposed_models": json.dumps(json_data.get("proposed_models", [])),
                "future_work": json_data.get("future_work", ""),
                "metadata": json.dumps(metadata),
            }
            records.append(record)

        # Insert records
        if records:
            # Build parameters for batch insert
            columns = config.schema_mapping.keys()
            insert_query = f"""
                INSERT INTO {config.table_name} 
                ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            """

            rows_inserted = duckdb_resource.execute_batch(
                insert_query, [[record.get(k) for k in columns] for record in records]
            )

            # Commit the transaction
            duckdb_resource.commit()

            context.log.info(
                f"Inserted {rows_inserted} records into {config.table_name}"
            )
        else:
            context.log.info("No records to insert")
            rows_inserted = 0

        return dg.Output(
            value={"rows_inserted": rows_inserted},
            metadata={
                "rows_inserted": MetadataValue.int(rows_inserted),
                "table_name": MetadataValue.text(config.table_name),
            },
        )

    except Exception as e:
        context.log.error(f"Database error: {str(e)}")
        raise
