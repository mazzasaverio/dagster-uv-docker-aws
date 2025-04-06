# src/assets/s2_structured_info.py
import dagster as dg
from typing import Dict, Any
from datetime import datetime
import json
from pathlib import Path

from services.structured_output_processor import process_content

from src.resources.storage import StorageResource
from src.utils.config_loader import load_prompt_config

logger = dg.get_dagster_logger()


class ExtractionConfig(dg.Config):
    input_folder: str = "s1_extract_pdf_text"
    output_folder: str = "s2_structured_info"


@dg.asset(
    group_name="reports",
    compute_kind="openai",
    deps=["extract_pdf_text"],
    code_version="v1",
)
async def extract_structured_info(
    context: dg.AssetExecutionContext,
    config: ExtractionConfig,
    storage: StorageResource,
    extract_pdf_text: Dict[str, Dict[str, str]],
) -> dg.Output[Dict[str, Dict]]:
    context.log.info("Starting data extraction")

    output_path = storage.get_full_path(config.output_folder)
    Path(output_path).mkdir(parents=True, exist_ok=True)

    config = load_prompt_config("s2_structured_info")
    llm_config = config.get("paper_information_extraction")

    structured_documents = {}

    for doc_id, doc_data in extract_pdf_text.items():
        try:

            doc_id = doc_data["filename"]

            content_text = doc_data["content"]

            json_data = await process_content(doc_id, content_text, llm_config)

            json_path = Path(output_path) / f"{Path(doc_id).stem}_structured.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)

            structured_documents[doc_id] = {
                "filename": doc_id,
                "extraction_date": doc_data["extraction_date"],
                "json_data": json_data,
            }

        except Exception as e:
            context.log.error(
                f"Error processing report {doc_data['filename']}: {str(e)}"
            )
            continue

    return dg.Output(
        value=structured_documents,
        metadata={
            "documents_processed": len(structured_documents),
            "success_rate": (
                f"{(len(structured_documents)/len(extract_pdf_text))*100:.2f}%"
                if extract_pdf_text
                else "0%"
            ),
            "output_path": output_path,
        },
    )
