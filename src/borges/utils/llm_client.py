"""LLM client utilities.

Talks to the OpenAI API or to any OpenAI-compatible server (vLLM, SGLang,
llama.cpp, Ollama) hosting an open-weight model. See docs/local-llm.md.
"""

import hashlib
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_community.callbacks import get_openai_callback
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, convert_to_messages
from langchain_openai import ChatOpenAI

from ..config import get_config
from .logging import get_logger

logger = get_logger(__name__)

_THINK_BLOCK = re.compile(r"<think>.*?</think>", re.DOTALL)


def strip_reasoning(text: str) -> str:
    """Remove the ``<think>...</think>`` block reasoning models write before answering.

    Qwen3, GLM and Kimi emit one unless thinking is disabled. Answers without a
    block are returned unchanged.
    """
    if "</think>" not in text:
        return text
    text = _THINK_BLOCK.sub("", text)
    # Some chat templates open the block in the prompt: only the closing tag is left
    if "</think>" in text:
        text = text.split("</think>", 1)[1]
    return text.strip()


class LLMClient:
    """Wrapper for LLM interactions with logging and error handling."""

    def __init__(
        self,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
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
        llm_config = config.api.llm

        self.model = model or llm_config.model
        self.temperature = (
            temperature if temperature is not None else llm_config.temperature
        )
        self.base_url = llm_config.base_url
        self.api_key = api_key or llm_config.api_key
        # Local servers ignore the key, but the OpenAI SDK insists on one
        if self.base_url and (not self.api_key or self.api_key.startswith("${")):
            self.api_key = "unused"
        self.timeout = timeout or llm_config.timeout
        self.max_retries = max_retries or llm_config.max_retries
        self.seed = llm_config.seed
        self.max_tokens = llm_config.max_tokens
        self.extra_body = llm_config.extra_body
        self.max_concurrency = max(1, llm_config.max_concurrency)
        self.structured_output = llm_config.structured_output
        self.cache_dir = Path(llm_config.cache_dir) if llm_config.cache_dir else None

        # Rate limiting settings
        self.request_delay = llm_config.request_delay
        self.retry_delay = llm_config.retry_delay

        # Initialize LLM
        self.llm = self._create_llm()

        # Track usage (requests may run in several threads)
        self._lock = threading.Lock()
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0
        self.cache_hits = 0
        self.total_seconds = 0.0
        self.last_request_time = 0.0

    def _create_llm(self) -> BaseChatModel:
        """Create LLM instance.

        Returns:
            LLM instance
        """
        # Only pass the optional settings that are set, so the default request
        # stays exactly the one used for the paper
        optional: Dict[str, Any] = {}
        if self.seed is not None:
            optional["seed"] = self.seed
        if self.max_tokens is not None:
            optional["max_tokens"] = self.max_tokens
        if self.extra_body:
            optional["extra_body"] = self.extra_body

        return ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            api_key=self.api_key,
            timeout=self.timeout,
            max_retries=self.max_retries,
            base_url=self.base_url,
            **optional,
        )

    def _cache_path(self, messages: List[Any], kwargs: Dict[str, Any]) -> Path:
        """Path of the cached response for this request.

        The key covers the model name and every setting that changes the answer,
        but not the server URL: serve different quantizations under different
        model names.
        """
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "seed": self.seed,
            "max_tokens": self.max_tokens,
            "extra_body": self.extra_body,
            "messages": [
                {"type": m.type, "content": m.content}
                for m in convert_to_messages(messages)
            ],
            "kwargs": kwargs,
        }
        key = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert self.cache_dir is not None
        return self.cache_dir / key[:2] / f"{key}.json"

    def _wait_turn(self) -> None:
        """Keep at least ``request_delay`` seconds between request starts."""
        if self.request_delay <= 0:
            return
        with self._lock:
            now = time.time()
            start = max(now, self.last_request_time + self.request_delay)
            self.last_request_time = start
        if start > now:
            logger.info(f"Rate limiting: sleeping for {start - now:.1f} seconds")
            time.sleep(start - now)

    def invoke(self, messages: List[Any], **kwargs) -> Any:
        """Invoke LLM with messages.

        Args:
            messages: List of messages
            **kwargs: Additional arguments for the request (e.g. response_format)

        Returns:
            LLM response
        """
        cache_path = self._cache_path(messages, kwargs) if self.cache_dir else None
        if cache_path is not None and cache_path.exists():
            cached = json.loads(cache_path.read_text())
            with self._lock:
                self.cache_hits += 1
            return AIMessage(content=cached["content"])

        self._wait_turn()

        logger.info("Invoking LLM", model=self.model, message_count=len(messages))

        try:
            started = time.time()
            with get_openai_callback() as cb:
                response = self.llm.invoke(messages, **kwargs)

            if isinstance(response.content, str):
                response.content = strip_reasoning(response.content)

            with self._lock:
                self.total_tokens += cb.total_tokens
                self.total_cost += cb.total_cost
                self.request_count += 1
                self.total_seconds += time.time() - started

            logger.info(
                "LLM invocation successful",
                model=self.model,
                tokens_used=cb.total_tokens,
                cost=cb.total_cost,
            )

            if cache_path is not None:
                self._write_cache(cache_path, response.content)

            return response

        except Exception as e:
            logger.error("LLM invocation failed", model=self.model, error=str(e))

            # If it's a rate limit error, wait longer before retrying
            if "429" in str(e) or "rate limit" in str(e).lower():
                logger.warning(
                    f"Rate limit hit, waiting {self.retry_delay} seconds before retry"
                )
                time.sleep(self.retry_delay)

            raise

    def _write_cache(self, path: Path, content: Any) -> None:
        """Store a response atomically, so a crash never leaves half a file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps({"model": self.model, "content": content}))
        tmp.replace(path)

    def batch_invoke(
        self,
        message_batches: List[List[Any]],
        batch_size: Optional[int] = None,
        max_concurrency: Optional[int] = None,
        return_exceptions: bool = False,
        **kwargs,
    ) -> List[Any]:
        """Invoke LLM with multiple message lists, ``max_concurrency`` at a time.

        Args:
            message_batches: List of message lists
            batch_size: Ignored; kept for compatibility
            max_concurrency: Requests in flight (default: api.llm.max_concurrency)
            return_exceptions: Return the exception of a failed request instead
                of None
            **kwargs: Additional arguments

        Returns:
            List of LLM responses, in input order
        """
        workers = max(1, max_concurrency or self.max_concurrency)

        logger.info(
            "Starting batch LLM invocation",
            total_requests=len(message_batches),
            max_concurrency=workers,
        )

        def call(messages: List[Any]) -> Any:
            try:
                return self.invoke(messages, **kwargs)
            except Exception as e:
                logger.error("Batch item failed", error=str(e))
                return e if return_exceptions else None

        if workers == 1:
            responses = [call(messages) for messages in message_batches]
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                responses = list(pool.map(call, message_batches))

        logger.info(
            "Batch LLM invocation complete",
            total_responses=len(responses),
            successful=sum(
                1 for r in responses if r is not None and not isinstance(r, Exception)
            ),
            total_tokens=self.total_tokens,
            total_cost=self.total_cost,
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
            "cache_hits": self.cache_hits,
            "total_tokens": self.total_tokens,
            "total_cost": self.total_cost,
            "total_seconds": self.total_seconds,
            "average_tokens_per_request": (
                self.total_tokens / self.request_count if self.request_count > 0 else 0
            ),
            "average_cost_per_request": (
                self.total_cost / self.request_count if self.request_count > 0 else 0
            ),
        }

    def reset_usage_stats(self) -> None:
        """Reset usage statistics."""
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0
        self.cache_hits = 0
        self.total_seconds = 0.0


def create_llm_client(use_vision: bool = False, **kwargs) -> LLMClient:
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
        kwargs.setdefault("model", config.api.llm.vision_model)

    return LLMClient(**kwargs)
