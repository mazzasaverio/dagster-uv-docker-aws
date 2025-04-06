from dagster import (
    asset,
    Output,
    MetadataValue,
    get_dagster_logger,
    Config,
    AssetExecutionContext,
)
from typing import Dict, Any
from pathlib import Path
import uuid
from unstructured.partition.pdf import partition_pdf
from datetime import datetime
import json

from src.resources.storage import StorageResource
from src.types.documents import ExtractedDocument, DocumentType

logger = get_dagster_logger()


class PDFExtractionConfig(Config):
    input_folder: str = "raw"
    output_folder: str = "s1_extract_pdf_text"
    batch_size: int = 10


@asset(
    compute_kind="pdf_extraction",
    group_name="documents",
    code_version="v1",
)
def extract_pdf_text(
    context: AssetExecutionContext,
    config: PDFExtractionConfig,
    storage: StorageResource,
) -> Output[Dict[str, Dict[str, str]]]:
    """Extract text from PDF files."""
    context.log.info("Starting PDF text extraction")

    input_path = storage.get_full_path(config.input_folder)
    output_path = storage.get_full_path(config.output_folder)
    context.log.info(f"Looking for PDFs in: {input_path}")
    context.log.info(f"Will save JSONs in: {output_path}")

    # Ensure output directory exists
    Path(output_path).mkdir(parents=True, exist_ok=True)

    pdf_files = list(Path(input_path).glob("**/*.pdf"))
    context.log.info(f"Found {len(pdf_files)} PDF files: {[f.name for f in pdf_files]}")

    if not pdf_files:
        context.log.warning(f"No PDF files found in {input_path}")
        return Output(value={}, metadata={"files_processed": 0})

    extracted_texts = {}

    for pdf_file in pdf_files:
        try:
            context.log.info(f"Processing {pdf_file.name}")
            elements = partition_pdf(filename=str(pdf_file))
            text_content = "\n".join([str(el) for el in elements])

            doc_id = str(uuid.uuid4())
            doc_data = {
                "filename": str(pdf_file.name),
                "content": text_content,
                "extraction_date": datetime.now().isoformat(),
            }

            # Save to JSON file with same name as PDF
            json_path = Path(output_path) / f"{pdf_file.stem}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(doc_data, f, ensure_ascii=False, indent=2)

            extracted_texts[doc_id] = doc_data
            context.log.info(
                f"Successfully processed {pdf_file.name} and saved to {json_path}"
            )

        except Exception as e:
            context.log.error(f"Error processing {pdf_file.name}: {str(e)}")
            context.log.exception("Full error:")
            continue

    return Output(
        value=extracted_texts,
        metadata={
            "files_processed": len(extracted_texts),
            "total_files": len(pdf_files),
            "success_rate": f"{(len(extracted_texts)/len(pdf_files))*100:.2f}%",
            "input_path": input_path,
            "output_path": output_path,
            "processed_files": [f.name for f in pdf_files],
        },
    )
