from typing import Generic, Type, Union

from brew_extract.core.transformers.abstract_data_source import AbstractDataSource
from brew_extract.core.transformers.abstract_transformer import AbstractTransformer
from brew_extract.core.transformers.transformer_stack import TransformerStack

from ..artifacts.abstract_artifact_generator import AbstractArtifactGenerator
from ..artifacts.artifact_cache_singleton import ArtifactCacheSingleton
from ..base_class import BaseClass
from ..errors import SchemaError
from ..type_vars import A, S, T


class PipelineBuilder(BaseClass, Generic[T, S]):

    id: str
    stacks: dict[str, TransformerStack[T, S]]
    artifact_cache: ArtifactCacheSingleton

    def __init__(self, stacks: dict[str, TransformerStack[T, S]], id: str = "default"):
        cache = ArtifactCacheSingleton()

        super().__init__(id=id, stacks=stacks, artifact_cache=cache)  # type: ignore
        # FIXME: mypy issue?

    @classmethod
    def empty(cls, id: str = "default") -> "PipelineBuilder":
        return cls(stacks={}, id=id)

    def add_source(self, source: AbstractDataSource[T, S], stack_id: str) -> None:
        new_stack = TransformerStack.empty(source=source)
        self.stacks[stack_id] = new_stack

    def add_transformer(
        self, transformer: AbstractTransformer[T, S], target_stack_id: str, source_stack_id: Union[str, None] = None
    ) -> None:
        if source_stack_id is None:
            source_stack_id = target_stack_id

        if source_stack_id not in self.stacks:
            raise ValueError(f"Source stack {source_stack_id} not found")

        new_stack = self.stacks[source_stack_id].with_new_transformer(transformer)

        if not new_stack.validate_schema():
            raise SchemaError(f"Schema validation failed for stack {source_stack_id}")

        self.stacks[target_stack_id] = new_stack

    def add_artifact(self, stack_id: str, artifact_class: Type[AbstractArtifactGenerator[T, S, A]]) -> None:

        artifact_generator = artifact_class(
            cache=self.artifact_cache,
            stack_id=stack_id,
            transformer_id=getattr(self.stacks.get(stack_id), "id", "0"),
        )

        return self.add_transformer(artifact_generator, target_stack_id=stack_id, source_stack_id=stack_id)

    def generate(self) -> dict[str, T]:
        results = {stack_id: stack.generate() for stack_id, stack in self.stacks.items()}
        return results

    def schemas(self) -> dict[str, S]:
        return {stack_id: stack.schema for stack_id, stack in self.stacks.items()}
