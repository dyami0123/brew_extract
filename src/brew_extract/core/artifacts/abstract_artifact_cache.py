from typing import Generic, Union

from ..base_class import BaseClass
from ..type_vars import A


class AbstractArtifactCache(BaseClass, Generic[A]):

    artifacts: dict[str, dict[str, A]]

    def cache(self, stack_id: str, transformer_id: str, artifact: A):
        if stack_id not in self.artifacts:
            self.artifacts[stack_id] = {}
        self.artifacts[stack_id][transformer_id] = artifact

    def get(self, transformer_id: str, stack_id: str) -> Union[A, None]:
        stack_cache = self.artifacts.get(stack_id, None)

        if stack_cache is None:
            self.logger.debug(f"Cache miss for stack {stack_id}")
            return None

        artifact = stack_cache.get(transformer_id, None)

        if artifact is None:
            self.logger.debug(f"Cache miss for transformer {transformer_id} in stack {stack_id}")
            return None

        return artifact

    @classmethod
    def empty(cls):
        return cls(artifacts={})
