from dagster import asset, Output, MetadataValue, Config, AssetExecutionContext
from pydantic import BaseModel
from typing import List, Dict, Any
import json
from resources.duckdb import DuckDBResource
from datetime import datetime


class DuckDBStorageConfig(Config):
    table_name: str = "documents"
    schema_mapping: Dict[str, str] = {
        "document_id": "VARCHAR",
        "filename": "VARCHAR",
        "title": "TEXT",
        "author": "VARCHAR",
        "content_summary": "TEXT",
        "metadata": "JSON",
    }


@asset(
    compute_kind="duckdb",
    group_name="documents",
    deps=["extract_structured_data"],
    code_version="v1",
)
def load_to_database(
    context: AssetExecutionContext,
    config: DuckDBStorageConfig,
    duckdb: DuckDBResource,
    extract_structured_data: Dict[str, Dict],
) -> Output[Dict[str, Any]]:
    """Load structured data into DuckDB database."""
    context.log.info("Starting database load")

    try:
        # Create table if not exists
        duckdb.create_table(config.table_name, config.schema_mapping)

        # Prepare records for insertion
        records = []
        for doc_id, doc_data in extract_structured_data.items():
            # Extract the extraction_date from doc_data
            extraction_date = (
                doc_data.pop("extraction_date")
                if "extraction_date" in doc_data
                else None
            )

            # Create metadata JSON
            metadata = {
                "extraction_date": extraction_date,
                "processing_date": datetime.now().isoformat(),
            }

            # Create the record
            record = {
                "document_id": doc_id,
                **doc_data,
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

            rows_inserted = duckdb.execute_batch(
                insert_query, [(record.get(k) for k in columns) for record in records]
            )

            # Commit the transaction
            duckdb.commit()

            context.log.info(
                f"Inserted {rows_inserted} records into {config.table_name}"
            )
        else:
            context.log.info("No records to insert")
            rows_inserted = 0

        return Output(
            value={"rows_inserted": rows_inserted},
            metadata={
                "rows_inserted": MetadataValue.int(rows_inserted),
                "table_name": MetadataValue.text(config.table_name),
            },
        )

    except Exception as e:
        context.log.error(f"Database error: {str(e)}")
        raise
