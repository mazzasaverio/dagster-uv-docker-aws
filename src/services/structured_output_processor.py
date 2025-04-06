import os
import json
from typing import Dict, Any
from dotenv import load_dotenv
from openai import AzureOpenAI
from dagster import get_dagster_logger

from src.services.llm_processor import LLMProcessor

load_dotenv()

logger = get_dagster_logger()


async def process_content(
    source_url: str, content_text: str, llm_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process job posting content using LLM to extract structured information.

    Args:
        source_url (str): The URL of the job posting
        content_text (str): The extracted text content from the job posting
        llm_config (Dict[str, Any]): Configuration for the LLM processing

    Returns:
        Dict[str, Any]: Structured job details in JSON format
    """

    logger.info(f"Processing job content from URL: {source_url}")

    logger.debug(f"Content text: {content_text}")

    logger.debug(f"LLM config: {llm_config}")

    # Initialize Azure OpenAI client
    client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2024-12-01-preview",
    )

    llm_processor = LLMProcessor(client, llm_config)

    response_content = await llm_processor.make_request(content_text)

    job_details = json.loads(response_content)

    logger.info(f"Successfully processed job content from {source_url}")
    return job_details
