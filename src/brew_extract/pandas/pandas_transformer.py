from typing import Callable

import pandas as pd

from brew_extract.core import AbstractTransformer

from .pandas_schema import PandasSchema


class PandasTransformer(AbstractTransformer[pd.DataFrame, PandasSchema]):

    transform_function: Callable[[pd.DataFrame], pd.DataFrame]
    schema_function: Callable[[PandasSchema], PandasSchema]


    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.transform_function(data)

    def transform_schema(self, schema: PandasSchema) -> PandasSchema:
        return self.schema_function(schema)
