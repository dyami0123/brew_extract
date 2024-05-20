import pandas as pd

from brew_extract.core import AbstractDataSource

from ..pandas_schema import PandasSchema


class AbstractPandasDataSource(AbstractDataSource[pd.DataFrame, PandasSchema]):
    
    def infer_schema(self, data: pd.DataFrame) -> PandasSchema:
        schema = data.dtypes.to_dict()
        return PandasSchema(schema)