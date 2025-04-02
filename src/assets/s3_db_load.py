from dagster import asset, Output, MetadataValue, Config, AssetExecutionContext
from pydantic import BaseModel
from typing import List, Dict, Any
import json
from src.resources.postgres import PostgreSQLResource
from datetime import datetime


class PostgreSQLStorageConfig(Config):
    table_name: str = "documents"
    schema_mapping: Dict[str, str] = {
        "document_id": "VARCHAR(255)",
        "filename": "VARCHAR(255)",
        "title": "TEXT",
        "author": "VARCHAR(255)",
        "content_summary": "TEXT",
        "metadata": "JSONB",
    }


@asset(
    compute_kind="postgres",
    group_name="documents",
    deps=["extract_structured_data"],
    code_version="v1",
)
def load_to_database(
    context: AssetExecutionContext,
    config: PostgreSQLStorageConfig,
    postgres: PostgreSQLResource,
    extract_structured_data: Dict[str, Dict],
) -> Output[Dict[str, Any]]:
    """Load structured data into PostgreSQL database."""
    context.log.info("Starting database load")

    try:
        # Create table if not exists
        postgres.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {config.table_name} (
                {', '.join(f'{k} {v}' for k, v in config.schema_mapping.items())}
            )
        """
        )

        # Prepare records for insertion
        records = [
            {
                "document_id": doc_id,
                **doc_data,
                "metadata": json.dumps(
                    {
                        "extraction_date": doc_data.pop("extraction_date"),
                        "processing_date": datetime.now().isoformat(),
                    }
                ),
            }
            for doc_id, doc_data in extract_structured_data.items()
        ]

        # Insert records
        insert_query = f"""
            INSERT INTO {config.table_name} 
            ({', '.join(config.schema_mapping.keys())})
            VALUES ({', '.join(['%s'] * len(config.schema_mapping))})
        """

        rows_inserted = postgres.execute_batch(
            insert_query,
            [(record[k] for k in config.schema_mapping.keys()) for record in records],
        )

        return Output(
            value={"rows_inserted": rows_inserted},
            metadata={"rows_inserted": rows_inserted, "table_name": config.table_name},
        )

    except Exception as e:
        context.log.error(f"Database error: {str(e)}")
        raise
