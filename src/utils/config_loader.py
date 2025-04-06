"""Utility module for loading configuration files."""

import os
import yaml
import logfire
from pathlib import Path
from typing import Dict, Any

# Get repository root path
repo_root = os.getenv(
    "PROJECT_ROOT",
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
)


def load_prompt_config(
    config_name: str = "job_extraction", section: str = None
) -> Dict[str, Any]:
    """Load prompt configuration from YAML file.

    Args:
        config_name (str): Name of the config file (without .yaml extension)
        section (str, optional): Section within the config to extract. If None, returns entire config.

    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    config_path = Path(repo_root) / "src" / "config" / f"{config_name}.yaml"

    logfire.info(f"Loading prompt config from {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        if section and section in config:
            config = config[section]

        return config
    except Exception as e:
        logfire.error(
            f"Error loading config from {config_path}: {str(e)}", exc_info=True
        )
        raise
