import logging
from typing import Any, Generic, Type, Union

from .base_class import BaseClass
from .type_vars import A


class AbstractArtifactCache(BaseClass, Generic[A]):

    artifacts: dict[str, dict[str, A]]

    def __init__(self, artifacts: dict[str, dict[str, A]]):
        self.artifacts = artifacts

    def cache(self, stack_id: str, transformer_id: str, artifact: A):
        if stack_id not in self.artifacts:
            self.artifacts[stack_id] = {}
        self.artifacts[stack_id][transformer_id] = artifact

    def get(self, transformer_id: str, stack_id: str) -> Union[A, None]:
        stack_cache = self.artifacts.get(stack_id, None)

        if stack_cache is None:
            logging.debug(f"Cache miss for stack {stack_id}")
            return None

        artifact = stack_cache.get(transformer_id, None)

        if artifact is None:
            logging.debug(f"Cache miss for transformer {transformer_id} in stack {stack_id}")
            return None

        return artifact

    @classmethod
    def empty(cls):
        return cls(artifacts={})


class TypeAgnosticArtifactCacheSingleton:

    _caches: dict[Type, AbstractArtifactCache]

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TypeAgnosticArtifactCacheSingleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self._caches = {}

    def cache(self, stack_id: str, transformer_id: str, artifact: A):
        cache = self._get_cache(type(artifact))
        cache.cache(stack_id, transformer_id, artifact)

    def get(self, transformer_id: str, stack_id: str, artifact_type: Type) -> Union[Any, None]:
        return self._get_cache(artifact_type).get(transformer_id, stack_id)

    def _get_cache(self, artifact_type: Type) -> AbstractArtifactCache:
        if artifact_type not in self._caches:
            self._caches[artifact_type] = AbstractArtifactCache.empty()
        return self._caches[artifact_type]
