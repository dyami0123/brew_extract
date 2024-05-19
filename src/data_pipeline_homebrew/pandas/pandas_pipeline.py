from typing import Callable, Type

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.figure import Figure

from ..abstractions import (
    AbstractArtifactGenerator,
    AbstractDataSource,
    AbstractTransformer,
    PipelineBuilder,
    TransformerStack,
    TypeAgnosticArtifactCacheSingleton,
)
from ..abstractions.type_vars import A


class PandasSchema:
    schema: dict[str, Type]

    def __init__(self, schema: dict[str, Type]):
        self.schema = schema


class LocalCSVDataSource(AbstractDataSource[pd.DataFrame, PandasSchema]):
    path: str

    def __init__(self, path: str):
        self.path = path

    def generate(self) -> pd.DataFrame:
        return pd.read_csv(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_csv(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)


class LocalParquetDataSource(AbstractDataSource[pd.DataFrame, PandasSchema]):
    path: str

    def __init__(self, path: str):
        self.path = path

    def generate(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_parquet(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)


class PandasTransformer(AbstractTransformer[pd.DataFrame, PandasSchema]):

    transform_function: Callable[[pd.DataFrame], pd.DataFrame]
    schema_function: Callable[[PandasSchema], PandasSchema]

    def __init__(
        self,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
        schema_function: Callable[[PandasSchema], PandasSchema],
    ):

        self.transform_function = transform_function
        self.schema_function = schema_function

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.transform_function(data)

    def transform_schema(self, schema: PandasSchema) -> PandasSchema:
        return self.schema_function(schema)


class GenericMatplotlibVisualization(AbstractArtifactGenerator[pd.DataFrame, PandasSchema, Figure]):
    @property
    def plot_function(self) -> Callable[[pd.DataFrame], Figure]:
        raise NotImplementedError

    def create_artifact(self, data: pd.DataFrame) -> Figure:
        return self.plot_function(data)


class SimpleLinePlot(GenericMatplotlibVisualization):
    @staticmethod
    def plot(data: pd.DataFrame) -> Figure:
        fig, ax = plt.subplots()

        data.plot(ax=ax)

        return fig

    @property
    def plot_function(self) -> Callable[[pd.DataFrame], Figure]:
        return self.plot


class PandaPipelineBuilder(PipelineBuilder[pd.DataFrame, PandasSchema]):
    def __init__(self, stacks: dict[str, TransformerStack[pd.DataFrame, PandasSchema]]):
        super().__init__(stacks=stacks)
        self.artifact_cache = TypeAgnosticArtifactCacheSingleton()

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
