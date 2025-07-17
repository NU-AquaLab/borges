"""LLM client utilities."""

from typing import Any, Dict, List, Optional

from langchain_community.callbacks import get_openai_callback
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config
from .logging import get_logger

logger = get_logger(__name__)


class LLMClient:
    """Wrapper for LLM interactions with logging and error handling."""

    def __init__(
        self,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None
    ):
        """Initialize LLM client.

        Args:
            model: Model name (uses config if not provided)
            temperature: Temperature setting
            api_key: API key (uses config if not provided)
            timeout: Request timeout
            max_retries: Maximum retries
        """
        config = get_config()
        llm_config = config.api.openai

        self.model = model or llm_config.model
        self.temperature = temperature if temperature is not None else llm_config.temperature
        self.api_key = api_key or llm_config.api_key
        self.timeout = timeout or llm_config.timeout
        self.max_retries = max_retries or llm_config.max_retries

        # Initialize LLM
        self.llm = self._create_llm()

        # Track usage
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0

    def _create_llm(self) -> BaseChatModel:
        """Create LLM instance.

        Returns:
            LLM instance
        """
        return ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            api_key=self.api_key,
            timeout=self.timeout,
            max_retries=self.max_retries
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def invoke(self, messages: List[Any], **kwargs) -> Any:
        """Invoke LLM with messages.

        Args:
            messages: List of messages
            **kwargs: Additional arguments

        Returns:
            LLM response
        """
        logger.info(
            "Invoking LLM",
            model=self.model,
            message_count=len(messages)
        )

        try:
            with get_openai_callback() as cb:
                response = self.llm.invoke(messages, **kwargs)

                # Track usage
                self.total_tokens += cb.total_tokens
                self.total_cost += cb.total_cost
                self.request_count += 1

                logger.info(
                    "LLM invocation successful",
                    model=self.model,
                    tokens_used=cb.total_tokens,
                    cost=cb.total_cost
                )

                return response

        except Exception as e:
            logger.error(
                "LLM invocation failed",
                model=self.model,
                error=str(e)
            )
            raise

    def batch_invoke(
        self,
        message_batches: List[List[Any]],
        batch_size: Optional[int] = None,
        **kwargs
    ) -> List[Any]:
        """Invoke LLM with multiple message batches.

        Args:
            message_batches: List of message lists
            batch_size: Batch size for processing
            **kwargs: Additional arguments

        Returns:
            List of LLM responses
        """
        config = get_config()
        batch_size = batch_size or config.processing.llm_batch_size

        responses = []
        total_batches = len(message_batches)

        logger.info(
            "Starting batch LLM invocation",
            total_batches=total_batches,
            batch_size=batch_size
        )

        # Process in batches
        for i in range(0, total_batches, batch_size):
            batch = message_batches[i:i + batch_size]

            logger.info(
                "Processing batch",
                batch_number=i // batch_size + 1,
                batch_size=len(batch)
            )

            # Process each message set in the batch
            batch_responses = []
            for messages in batch:
                try:
                    response = self.invoke(messages, **kwargs)
                    batch_responses.append(response)
                except Exception as e:
                    logger.error(
                        "Batch item failed",
                        error=str(e)
                    )
                    batch_responses.append(None)

            responses.extend(batch_responses)

        logger.info(
            "Batch LLM invocation complete",
            total_responses=len(responses),
            successful=sum(1 for r in responses if r is not None),
            total_tokens=self.total_tokens,
            total_cost=self.total_cost
        )

        return responses

    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics.

        Returns:
            Dictionary with usage stats
        """
        return {
            "model": self.model,
            "request_count": self.request_count,
            "total_tokens": self.total_tokens,
            "total_cost": self.total_cost,
            "average_tokens_per_request": (
                self.total_tokens / self.request_count if self.request_count > 0 else 0
            ),
            "average_cost_per_request": (
                self.total_cost / self.request_count if self.request_count > 0 else 0
            )
        }

    def reset_usage_stats(self) -> None:
        """Reset usage statistics."""
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0


def create_llm_client(
    use_vision: bool = False,
    **kwargs
) -> LLMClient:
    """Create LLM client from configuration.

    Args:
        use_vision: Whether to use vision model
        **kwargs: Override configuration values

    Returns:
        Configured LLM client
    """
    config = get_config()

    # Use vision model if requested
    if use_vision:
        kwargs.setdefault("model", config.api.openai.vision_model)

    return LLMClient(**kwargs)