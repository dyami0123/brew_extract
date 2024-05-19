from abc import ABC, abstractmethod
from typing import Generic

from .abstract_artifact_cache import TypeAgnosticArtifactCacheSingleton
from .abstract_transformer import AbstractTransformer
from .type_vars import A, S, T


class AbstractArtifactGenerator(AbstractTransformer[T, S], ABC, Generic[T, S, A]):
    cache: TypeAgnosticArtifactCacheSingleton
    stack_id: str
    transformer_id: str

    def __init__(self, cache: TypeAgnosticArtifactCacheSingleton, stack_id: str, transformer_id: str):
        self.cache = cache
        self.stack_id = stack_id
        self.transformer_id = transformer_id

    def transform(self, data: T) -> T:
        artifact = self.create_artifact(data)
        self.cache.cache(stack_id=self.stack_id, transformer_id=self.transformer_id, artifact=artifact)
        return data

    @abstractmethod
    def create_artifact(self, data: T) -> A:
        pass

    def transform_schema(self, schema: S) -> S:
        return schema
