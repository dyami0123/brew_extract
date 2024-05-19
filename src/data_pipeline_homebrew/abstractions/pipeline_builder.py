from typing import Generic, Type, Union

from .abstract_artifact_cache import TypeAgnosticArtifactCacheSingleton
from .abstract_artifact_generator import AbstractArtifactGenerator
from .abstract_data_source import AbstractDataSource
from .abstract_transformer import AbstractTransformer
from .base_class import BaseClass
from .errors import SchemaError
from .transformer_stack import TransformerStack
from .type_vars import A, S, T


class PipelineBuilder(BaseClass, Generic[T, S]):

    id: str
    stacks: dict[str, TransformerStack[T, S]]
    artifact_cache: TypeAgnosticArtifactCacheSingleton

    def __init__(self, stacks: dict[str, TransformerStack[T, S]], id: str = "default"):
        self.stacks = stacks
        self.visualization_cache = TypeAgnosticArtifactCacheSingleton()
        self.id = id

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


class CheckpointTransformer(AbstractTransformer[T, S], AbstractDataSource[T, S]):

    pipeline_id: str
    stack_id: str

    def __init__(self, pipeline_id: str, stack_id: str):
        self.pipeline_id = pipeline_id
        self.stack_id = stack_id

    def transform(self, data: T) -> T:
        if self._checkpoint_exists():
            return self._read_checkpoint()
        else:
            self._write_checkpoint(data)
        return data

    def transform_schema(self, schema: S) -> S:
        return schema

    
    def schema(self) -> S:
        return self.source.schema
    
    def _write_checkpoint(self, data: T) -> None:
        pass

    def _read_checkpoint(self) -> T:
        return None

    def checkpoint_exists(self) -> bool:
        return False

    def clear_checkpoint(self) -> None:
        pass


class CheckpointedPipelineBuilder(PipelineBuilder[T, S]):

    def __init__(self, stacks: dict[str, TransformerStack[T, S]]):
        super().__init__(stacks=stacks)

    def add_checkpoint(self, stack_id: str) -> None:
        checkpoint_transformer = CheckpointTransformer[T, S](pipeline_id=self.id, stack_id=stack_id)
        self.add_transformer(checkpoint_transformer, target_stack_id=stack_id)

    def generate(self) -> dict[str, T]:
        # find most recent checkpoint for each stack
        
    def _generate_stack(self, stack_id: str) -> T:
        
        reversed_stack = list(reversed(self.stacks[stack_id].transformers))
        
        stack_append = []
        
        for transformer in reversed_stack:
            stack_append.append(transformer)
            if isinstance(transformer, CheckpointTransformer):
                if transformer.checkpoint_exists():
                    break
        
        stack_append = list(reversed(stack_append))
        
        first_transformer = stack_append[0]
        if len(stack_append) == 1:
            return first_transformer.transform(first_transformer.source.generate())
        
        
    
    # def generate(self) -> dict[str, T]:
    #     results = {stack_id: stack.generate() for stack_id, stack in self.stacks.items()}
    #     self.artifact_cache.cache("results", "results", results)
    #     return results

    # def schemas(self) -> dict[str, S]:
    #     schemas = {stack_id: stack.schema for stack_id, stack in self.stacks.items()}
    #     self.artifact_cache.cache("schemas", "schemas", schemas)
    #     return schemas
