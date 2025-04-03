from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from src.resources.duckdb import DuckDBResource
from src.resources.storage import StorageResource, StorageType
from src.resources.openai import OpenAIResource
import os

from src.assets import s1_extract_pdf_text, s2_structured_info, s3_db_load


def get_resource_defs():
    deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")

    common_resources = {
        "duckdb": DuckDBResource(
            path=EnvVar("DUCKDB_PATH"),
        ),
        "openai": OpenAIResource(
            api_key=EnvVar("OPENAI_API_KEY"),
            model=os.getenv("OPENAI_MODEL", "gpt-4"),
        ),
    }

    env_specific_resources = {
        "local": {
            "storage": StorageResource(
                storage_type=StorageType.LOCAL,
                local_base_path=os.getenv("LOCAL_STORAGE_PATH", "./data"),
                s3_bucket_name=None,
            ),
        },
        "production": {
            "storage": StorageResource(
                storage_type=StorageType.S3,
                local_base_path=None,
                s3_bucket_name=EnvVar("S3_BUCKET_NAME"),
            ),
            "s3": S3Resource(
                region_name=os.getenv("AWS_REGION", "us-east-1"),
            ),
        },
    }

    return {**common_resources, **env_specific_resources[deployment_name]}


# Definizioni Dagster
defs = Definitions(
    assets=load_assets_from_modules(
        [s1_extract_pdf_text, s2_structured_info, s3_db_load]
    ),
    resources=get_resource_defs(),
    schedules=[
        ScheduleDefinition(
            name="daily_pdf_processing",
            cron_schedule="0 0 * * *",
            job=define_asset_job(
                name="daily_pdf_processing_job",
                selection="*",
            ),
        )
    ],
)
