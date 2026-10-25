"""One shared real embedding implementation for indexer and query API."""

import math
import os


class Embedder:
    def __init__(self):
        from fastembed import TextEmbedding

        self.model = TextEmbedding(
            model_name=os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"),
            cache_dir=os.getenv("EMBEDDING_CACHE", "data/generated/models"),
            threads=2,
        )

    def encode(self, texts: list[str]) -> list[list[float]]:
        vectors = []
        for embedding in self.model.embed(texts):
            values = embedding.tolist()
            norm = math.sqrt(sum(value * value for value in values))
            if not norm:
                raise ValueError("Embedding has zero norm")
            vectors.append([value / norm for value in values])
        return vectors
