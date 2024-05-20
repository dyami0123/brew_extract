from typing import Type

import pandas as pd

from ..core import AbstractArtifactGenerator, PipelineBuilder
from ..core.type_vars import A
from .pandas_schema import PandasSchema


class PandasPipelineBuilder(PipelineBuilder[pd.DataFrame, PandasSchema]):
    def add_artifact(
        self, stack_id: str, artifact_class: Type[AbstractArtifactGenerator[pd.DataFrame, PandasSchema, A]]
    ) -> None:

        artifact_generator = artifact_class(
            cache=self.artifact_cache,
            stack_id=stack_id,
            transformer_id=getattr(self.stacks.get(stack_id), "id", "0"),
        )

        return self.add_transformer(artifact_generator, target_stack_id=stack_id, source_stack_id=stack_id)

    def generate(self) -> dict[str, pd.DataFrame]:
        results = {stack_id: stack.generate() for stack_id, stack in self.stacks.items()}
        return results

    def schemas(self) -> dict[str, PandasSchema]:
        return {stack_id: stack.schema for stack_id, stack in self.stacks.items()}
