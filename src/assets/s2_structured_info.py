from dagster import (
    asset,
    Config,
    AssetExecutionContext,
    AssetIn,
    get_dagster_logger,
    Output,
    MetadataValue,
)
from typing import Dict, Any

from src.resources.storage import StorageResource
from src.resources.openai import OpenAIResource
from src.types.documents import ExtractedDocument, StructuredDocument

logger = get_dagster_logger()


class StructuredExtractionConfig(Config):
    input_folder: str = "extracted"
    output_folder: str = "structured"
    batch_size: int = 10


@asset(
    group_name="documents",
    compute_kind="openai",
    deps=["extract_pdf_text"],
    code_version="v1",
)
def extract_structured_data(
    context: AssetExecutionContext,
    config: StructuredExtractionConfig,
    storage: StorageResource,
    openai: OpenAIResource,
    extract_pdf_text: Dict[str, Dict[str, str]],
) -> Output[Dict[str, Dict]]:
    """Extract structured data using OpenAI."""
    context.log.info("Starting structured data extraction")

    structured_documents = {}

    for doc_id, doc_data in extract_pdf_text.items():
        try:
            context.log.info(f"Processing document {doc_data['filename']}")

            structured_data = openai.extract_structured_data(
                text=doc_data["content"],
                output_schema={
                    "title": "string",
                    "author": "string",
                    "content_summary": "string",
                },
            )

            structured_documents[doc_id] = {
                "filename": doc_data["filename"],
                "extraction_date": doc_data["extraction_date"],
                **structured_data,
            }

        except Exception as e:
            context.log.error(f"Error processing {doc_data['filename']}: {str(e)}")
            continue

    return Output(
        value=structured_documents,
        metadata={
            "documents_processed": len(structured_documents),
            "success_rate": f"{(len(structured_documents)/len(extract_pdf_text))*100:.2f}%",
        },
    )
