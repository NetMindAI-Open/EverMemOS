"""
DeepInfra Rerank Service Implementation

Reranking service using DeepInfra commercial API.
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
class DeepInfraRerankConfig:
    """DeepInfra rerank service configuration"""

    api_key: str = ""
    base_url: str = "https://api.deepinfra.com/v1/inference"
    model: str = "Qwen/Qwen3-Reranker-4B"
    timeout: int = 30
    max_retries: int = 3
    batch_size: int = 10
    max_concurrent_requests: int = 5


class DeepInfraRerankService(RerankServiceInterface):
    """DeepInfra reranking service implementation"""

    def __init__(self, config: Optional[DeepInfraRerankConfig] = None):
        if config is None:
            config = DeepInfraRerankConfig()

        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        logger.info(
            f"Initialized DeepInfraRerankService | model={config.model}"
        )

    async def _ensure_session(self):
        """Ensure HTTP session is created"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
            )

    async def close(self):
        """Close HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

    def _format_rerank_texts(
        self, query: str, documents: List[str], instruction: Optional[str] = None
    ):
        """Format rerank request texts (Qwen-Reranker format)"""
        prefix = '<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
        suffix = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
        instruction = (
            instruction
            or "Given a question and a passage, determine if the passage contains information relevant to answering the question."
        )

        formatted_query = f"{prefix}<Instruct>: {instruction}\n<Query>: {query}\n"
        formatted_docs = [f"<Document>: {doc}{suffix}" for doc in documents]

        return [formatted_query] * len(documents), formatted_docs

    async def _send_rerank_request_batch(
        self,
        query: str,
        documents: List[str],
        start_index: int,
        instruction: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send rerank request batch to DeepInfra API"""
        await self._ensure_session()

        # Format texts
        queries, formatted_docs = self._format_rerank_texts(
            query, documents, instruction
        )

        url = self.config.base_url
        if not url.endswith(self.config.model):
            url = f"{url}/{self.config.model}"

        request_data = {"queries": queries, "documents": formatted_docs}

        async with self._semaphore:
            for attempt in range(self.config.max_retries):
                try:
                    async with self.session.post(url, json=request_data) as response:
                        if response.status == 200:
                            json_body = await response.json()
                            return self._parse_response(json_body)
                        else:
                            error_text = await response.text()
                            logger.error(
                                f"DeepInfra rerank API error {response.status}: {error_text}"
                            )
                            if attempt < self.config.max_retries - 1:
                                await asyncio.sleep(2**attempt)
                                continue
                            raise RerankError(
                                f"API failed: {response.status} - {error_text}"
                            )
                except Exception as e:
                    logger.error(f"DeepInfra rerank exception: {e}")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    raise RerankError(f"Exception: {e}")

    def _parse_response(self, json_body: Dict[str, Any]) -> Dict[str, Any]:
        """Parse DeepInfra API response"""
        scores = []
        if "results" in json_body:
            results = json_body["results"]
            results.sort(key=lambda x: x.get("index", 0))
            scores = [item.get("relevance_score", 0.0) for item in results]
        elif "scores" in json_body:
            scores = json_body["scores"]

        return {
            "scores": scores,
            "input_tokens": json_body.get("usage", {}).get("prompt_tokens", 0),
            "request_id": json_body.get("id"),
        }

    async def _make_rerank_request(
        self, query: str, documents: List[str], instruction: str = None
    ) -> Dict[str, Any]:
        """Make rerank request (with batching support)"""
        if not documents:
            return {"results": []}

        # Split into batches
        batch_size = self.config.batch_size
        if batch_size <= 0:
            batch_size = 10

        batches = [
            documents[i : i + batch_size] for i in range(0, len(documents), batch_size)
        ]

        batch_tasks = []
        for i, batch in enumerate(batches):
            start_index = i * batch_size
            batch_tasks.append(
                self._send_rerank_request_batch(query, batch, start_index, instruction)
            )

        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        all_scores = []
        total_input_tokens = 0
        last_response = None

        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                logger.error(f"Rerank batch {i} failed: {result}")
                batch_len = len(batches[i])
                all_scores.extend([-100.0] * batch_len)
                continue

            scores = result.get("scores", [])
            all_scores.extend(scores)
            total_input_tokens += result.get("input_tokens", 0)
            last_response = result

        combined_response = {
            "scores": all_scores,
            "input_tokens": total_input_tokens,
            "request_id": last_response.get("request_id") if last_response else None,
        }
        return self._convert_response_format(combined_response, len(documents))

    def _convert_response_format(
        self, combined_response: Dict[str, Any], num_documents: int
    ) -> Dict[str, Any]:
        """Convert response to standard format"""
        scores = combined_response.get("scores", [])
        if len(scores) < num_documents:
            scores.extend([0.0] * (num_documents - len(scores)))
        scores = scores[:num_documents]

        indexed_scores = [(i, score) for i, score in enumerate(scores)]
        indexed_scores.sort(key=lambda x: x[1], reverse=True)

        results = []
        for rank, (original_index, score) in enumerate(indexed_scores):
            results.append(
                {"index": original_index, "score": score, "rank": rank}
            )

        return {
            "results": results,
            "input_tokens": combined_response.get("input_tokens", 0),
            "request_id": combined_response.get("request_id"),
        }

    def _extract_memory_text(self, memory: Any) -> str:
        """Extract text from memory object"""
        if hasattr(memory, 'episode') and memory.episode:
            return memory.episode
        elif hasattr(memory, 'summary') and memory.summary:
            return memory.summary
        elif hasattr(memory, 'subject') and memory.subject:
            return memory.subject
        return str(memory)

    def _extract_text_from_hit(self, hit: Dict[str, Any]) -> str:
        """Extract and concatenate text based on memory_type"""
        source = hit.get('_source', hit)
        memory_type = hit.get('memory_type', '')

        # Extract text based on memory_type
        match memory_type:
            case MemoryType.EPISODIC_MEMORY.value:
                episode = source.get('episode', '')
                if episode:
                    return f"Episode Memory: {episode}"
            case MemoryType.FORESIGHT.value:
                foresight = source.get('foresight', '') or source.get('content', '')
                evidence = source.get('evidence', '')
                if foresight:
                    if evidence:
                        return f"Foresight: {foresight} (Evidence: {evidence})"
                    return f"Foresight: {foresight}"
            case MemoryType.EVENT_LOG.value:
                atomic_fact = source.get('atomic_fact', '')
                if atomic_fact:
                    return f"Atomic Fact: {atomic_fact}"

        # Generic fallback
        if source.get('episode'): return source['episode']
        if source.get('atomic_fact'): return source['atomic_fact']
        if source.get('foresight'): return source['foresight']
        if source.get('content'): return source['content']
        if source.get('summary'): return source['summary']
        if source.get('subject'): return source['subject']
        return str(hit)

    async def rerank_memories(
        self, query: str, retrieve_response: Any, instruction: str = None
    ) -> Union[RerankMemResponse, List[Dict[str, Any]]]:
        """Rerank memories using DeepInfra API"""

        # 1. Handle List of hits (raw dicts)
        if isinstance(retrieve_response, list):
            return await self._rerank_all_hits(query, retrieve_response, instruction=instruction)

        # 2. Handle RetrieveMemResponse object
        if not hasattr(retrieve_response, 'memories') or not retrieve_response.memories:
            return RerankMemResponse(memories=[], scores=[])

        all_memories_meta = []
        all_texts = []

        for group_idx, memory_dict_by_group in enumerate(retrieve_response.memories):
            for group_id, memory_list in memory_dict_by_group.items():
                for mem_idx, memory in enumerate(memory_list):
                    all_memories_meta.append((group_idx, group_id, mem_idx, memory))
                    all_texts.append(self._extract_memory_text(memory))

        if not all_texts:
            return RerankMemResponse(
                memories=retrieve_response.memories,
                scores=retrieve_response.scores,
                total_count=getattr(retrieve_response, 'total_count', 0),
            )

        try:
            rerank_result = await self._make_rerank_request(
                query, all_texts, instruction
            )
            results_meta = rerank_result.get("results", [])

            group_data_map = {}

            for item in results_meta:
                original_idx = item["index"]
                score = item["score"]

                group_idx, group_id, mem_idx, memory = all_memories_meta[original_idx]

                if group_id not in group_data_map:
                    group_data_map[group_id] = {
                        "memories": [],
                        "scores": [],
                        "rerank_scores": [],
                        "original_data": [],
                    }

                orig_score = 0.0
                if group_idx < len(retrieve_response.scores):
                    g_scores = retrieve_response.scores[group_idx].get(group_id, [])
                    if mem_idx < len(g_scores):
                        orig_score = g_scores[mem_idx]

                orig_data = {}
                if hasattr(retrieve_response, 'original_data') and group_idx < len(
                    retrieve_response.original_data
                ):
                    g_data = retrieve_response.original_data[group_idx].get(
                        group_id, []
                    )
                    if mem_idx < len(g_data):
                        orig_data = g_data[mem_idx]

                group_data_map[group_id]["memories"].append(memory)
                group_data_map[group_id]["scores"].append(orig_score)
                group_data_map[group_id]["rerank_scores"].append(score)
                group_data_map[group_id]["original_data"].append(orig_data)

            final_memories = [{gid: d["memories"] for gid, d in group_data_map.items()}]
            final_scores = [{gid: d["scores"] for gid, d in group_data_map.items()}]
            final_rerank_scores = [
                {gid: d["rerank_scores"] for gid, d in group_data_map.items()}
            ]
            final_original_data = [
                {gid: d["original_data"] for gid, d in group_data_map.items()}
            ]

            return RerankMemResponse(
                memories=final_memories,
                scores=final_scores,
                rerank_scores=final_rerank_scores,
                original_data=final_original_data,
                total_count=len(all_texts),
                has_more=getattr(retrieve_response, 'has_more', False),
                metadata=getattr(retrieve_response, 'metadata', {}),
            )

        except Exception as e:
            logger.error(f"Rerank object failed: {e}")
            return RerankMemResponse(
                memories=retrieve_response.memories,
                scores=retrieve_response.scores,
                total_count=getattr(retrieve_response, 'total_count', 0),
            )

    async def _rerank_all_hits(
        self,
        query: str,
        all_hits: List[Dict[str, Any]],
        top_k: int = None,
        instruction: str = None,
    ) -> List[Dict[str, Any]]:
        """Rerank the all_hits list and return top_k results"""
        if not all_hits:
            return []

        # Extract text content from all_hits for reranking
        all_texts = []
        for hit in all_hits:
            text = self._extract_text_from_hit(hit)
            all_texts.append(text)

        if not all_texts:
            return []

        # Call reranking API
        try:
            logger.debug(
                f"Starting reranking, query text: {query}, number of texts: {len(all_texts)}"
            )
            rerank_result = await self._make_rerank_request(
                query, all_texts, instruction
            )

            if "results" not in rerank_result:
                raise RerankError("Invalid rerank API response: missing results field")

            # Parse reranking results
            results_meta = rerank_result.get("results", [])

            # Reorganize hits according to reranked order
            reranked_hits = []
            for item in results_meta:
                original_idx = item.get("index", 0)
                score = item.get("score", 0.0)
                if 0 <= original_idx < len(all_hits):
                    hit = all_hits[original_idx].copy()
                    hit['score'] = score  # Unified score field
                    reranked_hits.append(hit)

            # If top_k is specified, return only the top_k results
            if top_k is not None and top_k > 0:
                reranked_hits = reranked_hits[:top_k]

            # Print top 3 result scores for debugging
            if reranked_hits:
                top_scores = [f"{h.get('score', 0):.4f}" for h in reranked_hits[:3]]
                logger.info(f"Reranking completed: {len(reranked_hits)} results, top scores: {top_scores}")
            return reranked_hits

        except Exception as e:
            logger.error(f"Error during reranking all_hits: {e}")
            # If reranking fails, return original results (sorted by original score)
            sorted_hits = sorted(
                all_hits, key=lambda x: x.get('score', 0), reverse=True
            )
            if top_k is not None and top_k > 0:
                sorted_hits = sorted_hits[:top_k]
            return sorted_hits

