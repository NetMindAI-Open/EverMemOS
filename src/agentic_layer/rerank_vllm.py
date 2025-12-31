"""
vLLM (Self-Deployed) Rerank Service Implementation

Reranking service for self-deployed vLLM or similar OpenAI-compatible services.
"""

import os
import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass

from agentic_layer.rerank_interface import (
    RerankServiceInterface,
    RerankError,
    RerankMemResponse,
)
from api_specs.memory_models import MemoryType

logger = logging.getLogger(__name__)


@dataclass
class VllmRerankConfig:
    """vLLM rerank service configuration"""

    api_key: str = "EMPTY"
    base_url: str = "http://localhost:12000/v1/rerank"
    model: str = "Qwen/Qwen3-Reranker-4B"
    timeout: int = 30
    max_retries: int = 3
    batch_size: int = 10
    max_concurrent_requests: int = 5


class VllmRerankService(RerankServiceInterface):
    """vLLM reranking service implementation"""

    def __init__(self, config: Optional[VllmRerankConfig] = None):
        if config is None:
            config = VllmRerankConfig()

        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        logger.info(
            f"Initialized VllmRerankService | url={config.base_url} | model={config.model}"
        )

    async def _ensure_session(self):
        """Ensure HTTP session is created"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            headers = {"Content-Type": "application/json"}
            if self.config.api_key and self.config.api_key != "EMPTY":
                headers["Authorization"] = f"Bearer {self.config.api_key}"
            self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def close(self):
        """Close HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def _send_rerank_request_batch(
        self,
        query: str,
        documents: List[str],
        start_index: int,
        instruction: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send rerank request batch to vLLM rerank API (OpenAI-compatible format)"""
        await self._ensure_session()

        url = self.config.base_url
        # Use OpenAI-compatible rerank API format
        request_data = {
            "model": self.config.model,
            "query": query,
            "documents": documents,
        }

        if instruction:
            request_data["instruction"] = instruction

        async with self._semaphore:
            for attempt in range(self.config.max_retries):
                try:
                    async with self.session.post(url, json=request_data) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result
                        else:
                            error_text = await response.text()
                            logger.warning(
                                f"vLLM rerank API error (status {response.status}, attempt {attempt + 1}/{self.config.max_retries}): {error_text}"
                            )
                            if attempt < self.config.max_retries - 1:
                                await asyncio.sleep(2**attempt)
                                continue
                            else:
                                raise RerankError(
                                    f"Rerank request failed after {self.config.max_retries} attempts: {error_text}"
                                )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"vLLM rerank timeout (attempt {attempt + 1}/{self.config.max_retries})"
                    )
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    else:
                        raise RerankError(
                            f"Rerank request timed out after {self.config.max_retries} attempts"
                        )
                except aiohttp.ClientError as e:
                    logger.warning(
                        f"vLLM rerank client error (attempt {attempt + 1}/{self.config.max_retries}): {e}"
                    )
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    else:
                        raise RerankError(
                            f"Rerank request failed after {self.config.max_retries} attempts: {e}"
                        )
                except Exception as e:
                    logger.error(f"Unexpected error in vLLM rerank request: {e}")
                    raise RerankError(f"Unexpected rerank error: {e}")

    async def rerank_memories(
        self,
        query: str,
        memories: List[RerankMemResponse],
        memory_type: MemoryType,
        top_k: Optional[int] = None,
        instruction: Optional[str] = None,
    ) -> List[RerankMemResponse]:
        """
        Rerank memories using vLLM reranking service

        Args:
            query: Search query
            memories: List of memories to rerank
            memory_type: Type of memory (episodic or semantic)
            top_k: Return top K results (optional)
            instruction: Optional instruction for reranking

        Returns:
            List of reranked memories
        """
        if not memories:
            return []

        # Prepare documents for reranking
        documents = []
        for mem in memories:
            if memory_type == MemoryType.EPISODIC:
                # For episodic: use content
                doc_text = mem.content or ""
            else:  # SEMANTIC
                # For semantic: use memory_text
                doc_text = mem.memory_text or ""
            documents.append(doc_text)

        # Send rerank request
        try:
            result = await self._send_rerank_request_batch(
                query=query,
                documents=documents,
                start_index=0,
                instruction=instruction,
            )

            # Parse results (OpenAI-compatible format)
            if "results" not in result:
                raise RerankError(f"Invalid rerank response format: missing 'results' key")

            # Create score mapping
            score_map = {}
            for item in result["results"]:
                index = item.get("index")
                score = item.get("relevance_score", 0.0)
                if index is not None:
                    score_map[index] = score

            # Update memory scores
            reranked_memories = []
            for i, mem in enumerate(memories):
                if i in score_map:
                    mem.rerank_score = score_map[i]
                    reranked_memories.append(mem)

            # Sort by rerank score (descending)
            reranked_memories.sort(key=lambda x: x.rerank_score or 0.0, reverse=True)

            # Apply top_k if specified
            if top_k is not None and top_k > 0:
                reranked_memories = reranked_memories[:top_k]

            logger.info(
                f"Reranked {len(memories)} memories -> {len(reranked_memories)} results | "
                f"memory_type={memory_type.value} | top_k={top_k}"
            )
            return reranked_memories

        except Exception as e:
            logger.error(f"Error in rerank_memories: {e}")
            raise RerankError(f"Failed to rerank memories: {e}")

    async def rerank_documents(
        self,
        query: str,
        documents: List[str],
        top_k: Optional[int] = None,
        instruction: Optional[str] = None,
    ) -> List[Dict[str, Union[int, str, float]]]:
        """
        Rerank documents using vLLM reranking service

        Args:
            query: Search query
            documents: List of document texts
            top_k: Return top K results (optional)
            instruction: Optional instruction for reranking

        Returns:
            List of reranked documents with scores
        """
        if not documents:
            return []

        try:
            result = await self._send_rerank_request_batch(
                query=query,
                documents=documents,
                start_index=0,
                instruction=instruction,
            )

            # Parse results (OpenAI-compatible format)
            if "results" not in result:
                raise RerankError(f"Invalid rerank response format: missing 'results' key")

            # Format results
            reranked_docs = []
            for item in result["results"]:
                doc_dict = {
                    "index": item.get("index"),
                    "document": item.get("document", {}).get("text", ""),
                    "relevance_score": item.get("relevance_score", 0.0),
                }
                reranked_docs.append(doc_dict)

            # Sort by relevance score (descending)
            reranked_docs.sort(key=lambda x: x["relevance_score"], reverse=True)

            # Apply top_k if specified
            if top_k is not None and top_k > 0:
                reranked_docs = reranked_docs[:top_k]

            logger.info(
                f"Reranked {len(documents)} documents -> {len(reranked_docs)} results | top_k={top_k}"
            )
            return reranked_docs

        except Exception as e:
            logger.error(f"Error in rerank_documents: {e}")
            raise RerankError(f"Failed to rerank documents: {e}")

    def get_model_name(self) -> str:
        """Get the current model name"""
        return self.config.model

