from dagster import ConfigurableResource, get_dagster_logger
from typing import Dict, Any, Optional

logger = get_dagster_logger()


class OpenAIResource(ConfigurableResource):
    """Resource for interacting with OpenAI API."""

    api_key: str
    model: str = "gpt-4"
    temperature: float = 0.0
    max_tokens: int = 1000

    def setup_for_execution(self, _) -> None:
        """Initialize OpenAI client."""
        import openai

        self._client = openai.Client(api_key=self.api_key)

    def extract_structured_data(
        self,
        text: str,
        output_schema: Dict[str, Any],
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Extract structured data from text using OpenAI's chat completion API.

        Args:
            text (str): The input text to process.
            output_schema (Dict[str, Any]): The schema for the structured output.
            system_prompt (Optional[str]): A custom system prompt for the AI model.

        Returns:
            Dict[str, Any]: Structured data extracted from the input text.
        """
        if not hasattr(self, "_client"):
            raise RuntimeError(
                "OpenAI client is not initialized. Did you call setup_for_execution?"
            )

        default_prompt = (
            "You are a helpful assistant that extracts structured information from text. "
            "Use the provided schema to format your response. If a field cannot be extracted, use null."
        )

        messages = [
            {"role": "system", "content": system_prompt or default_prompt},
            {"role": "user", "content": f"Schema: {output_schema}\n\nText:\n{text}"},
        ]

        try:
            logger.info("Sending request to OpenAI...")
            response = self._client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )

            logger.info("Response received from OpenAI.")
            return response["choices"][0]["message"]["content"]

        except Exception as e:
            logger.error(f"Error during OpenAI API call: {e}")
            raise
