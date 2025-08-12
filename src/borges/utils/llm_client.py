"""LLM client utilities."""

import time
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
        
        # Rate limiting settings
        self.request_delay = getattr(llm_config, 'request_delay', 0)
        self.retry_delay = getattr(llm_config, 'retry_delay', 60)

        # Initialize LLM
        self.llm = self._create_llm()

        # Track usage
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0
        self.last_request_time = 0

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

    def invoke(self, messages: List[Any], **kwargs) -> Any:
        """Invoke LLM with messages.

        Args:
            messages: List of messages
            **kwargs: Additional arguments

        Returns:
            LLM response
        """
        # Apply rate limiting delay
        if self.request_delay > 0:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.request_delay:
                sleep_time = self.request_delay - time_since_last
                logger.info(f"Rate limiting: sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
        
        logger.info(
            "Invoking LLM",
            model=self.model,
            message_count=len(messages)
        )

        try:
            with get_openai_callback() as cb:
                response = self.llm.invoke(messages, **kwargs)

                # Track usage and request time
                self.total_tokens += cb.total_tokens
                self.total_cost += cb.total_cost
                self.request_count += 1
                self.last_request_time = time.time()

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
            
            # If it's a rate limit error, wait longer before retrying
            if "429" in str(e) or "rate limit" in str(e).lower():
                logger.warning(f"Rate limit hit, waiting {self.retry_delay} seconds before retry")
                time.sleep(self.retry_delay)
                
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