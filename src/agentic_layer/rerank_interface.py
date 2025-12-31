"""
Rerank Service Interface

Defines the abstract interface for all reranking service implementations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union
from dataclasses import dataclass, field


class RerankError(Exception):
    """Rerank API error exception class"""
    pass


@dataclass
class RerankMemResponse:
    """Reranked memory retrieval response"""

    memories: List[Dict[str, List[Any]]] = field(default_factory=list)
    scores: List[Dict[str, List[float]]] = field(default_factory=list)
    rerank_scores: List[Dict[str, List[float]]] = field(default_factory=list)
    importance_scores: List[float] = field(default_factory=list)
    original_data: List[Dict[str, List[Dict[str, Any]]]] = field(default_factory=list)
    total_count: int = 0
    has_more: bool = False
    query_metadata: Any = field(default_factory=dict)
    metadata: Any = field(default_factory=dict)


class RerankServiceInterface(ABC):
    """Reranking service interface"""

    @abstractmethod
    async def rerank_memories(
        self, query: str, retrieve_response: Any, instruction: str = None
    ) -> Union[RerankMemResponse, List[Dict[str, Any]]]:
        """
        Rerank memories based on query
        
        Args:
            query: Query text
            retrieve_response: Retrieved memories (can be RetrieveMemResponse or List[Dict])
            instruction: Optional reranking instruction
            
        Returns:
            Reranked memories (RerankMemResponse or List[Dict[str, Any]])
        """
        pass

    @abstractmethod
    async def close(self):
        """Close and cleanup resources"""
        pass

