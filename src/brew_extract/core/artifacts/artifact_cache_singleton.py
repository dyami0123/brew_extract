from typing import Any, Type, Union

from ..type_vars import A
from .abstract_artifact_cache import AbstractArtifactCache


class ArtifactCacheSingleton:

    _caches: dict[Type, AbstractArtifactCache]

    def __new__(cls, *args, **kwargs):
        if not getattr(cls, "_instance", None):
            cls._instance = super(ArtifactCacheSingleton, cls).__new__(cls, *args, **kwargs)
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
